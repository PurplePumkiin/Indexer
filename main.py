import os
import re
import tarfile
import hashlib
import logging
import time
import urllib.parse
import sys
import sqlite3
from html.parser import HTMLParser
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

# =========================
# Configuration constants
# =========================
MAX_BATCH = 100                      # Pages per batch
BACKOFF_TIME = 3600                  # seconds for 429 (rate limit)
SERVER_ERR_BACKOFF = 900             # seconds for 5xx
RETRY_LIMIT = 5                      # retries per URL on transient errors
PAGES_PER_ARCHIVE = 10000            # rotate after this many saved files
PAGES_DIR = "data/pages/"
ARCHIVE_DIR = "data/archives/"
LOG_FILE = "logs/scraper.log"
USER_AGENT = "SimpleScape/1.0 (+https://map.pumkiinpatch.com)"
REQUEST_TIMEOUT = 10                 # seconds
MAX_BYTES = 10 * 1024 * 1024         # 10 MB safety cap on response bodies
STARTING_PAGE = "https://en.wikipedia.org/wiki/Working_set"

# Ensure directories exist BEFORE logging
for path in [PAGES_DIR, ARCHIVE_DIR, os.path.dirname(LOG_FILE)]:
    os.makedirs(path, exist_ok=True)

# Create the root logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create a file handler
file_handler = logging.FileHandler("logs/scraper.log")
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
file_handler.setFormatter(file_formatter)

# Create a console (stdout) handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
console_handler.setFormatter(console_formatter)

# Add both handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# =========================
# Database setup (SQLite WAL)
# =========================
conn = sqlite3.connect('crawler.db', isolation_level=None)
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA synchronous=NORMAL;")

def init_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS hosts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            host TEXT UNIQUE NOT NULL,
            backoff INTEGER DEFAULT 0,
            backoff_time INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS main (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            host TEXT NOT NULL,
            url TEXT UNIQUE NOT NULL,
            copyof TEXT,
            ref_host TEXT,
            ref_url TEXT,
            first_seen INTEGER,
            last_seen INTEGER,
            next_seen INTEGER,
            response INTEGER,
            response_hash TEXT
        )
    """)
    # Helpful indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_main_first_next ON main(first_seen, next_seen)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_main_host ON main(host)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_hosts_backoff ON hosts(backoff, backoff_time)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_main_hash ON main(response_hash)")
init_db(conn)

# =========================
# Utilities
# =========================
def now_ts() -> int:
    return int(time.time())

def url_host(u: str) -> str:
    try:
        return urllib.parse.urlparse(u).netloc.lower()
    except Exception:
        return ""

def normalize_url_strip_query(u: str) -> str:
    """
    Your rule: find links and strip query parameters so it’s just URL + URI.
    - Lowercase scheme/host
    - Strip fragment
    - Remove query entirely
    - Keep path (collapse empty path to '/')
    - Remove default ports
    """
    p = urllib.parse.urlparse(u)
    if p.scheme not in ("http", "https"):
        return ""  # ignore non-http(s)
    host = p.hostname.lower() if p.hostname else ""
    port = f":{p.port}" if p.port else ""
    # Drop default ports
    if (p.scheme == "http" and p.port == 80) or (p.scheme == "https" and p.port == 443):
        port = ""
    path = p.path or "/"
    # Remove duplicate slashes in path (but keep leading '/')
    path = re.sub(r'/{2,}', '/', path)
    # Drop params/query/fragment
    return urllib.parse.urlunparse((p.scheme, host + port, path, "", "", ""))

def ensure_host_row(h: str):
    conn.execute("INSERT OR IGNORE INTO hosts (host, backoff, backoff_time) VALUES (?, 0, NULL)", (h,))

def seed_if_empty(start_url: str):
    c = conn.execute("SELECT COUNT(*) FROM main").fetchone()[0]
    if c == 0:
        h = url_host(start_url)
        ensure_host_row(h)
        # First enqueue uses first_seen = now (you said First Seen = first time the page was visited,
        # but we need a way to pull "things to do". We'll rely on first_seen IS NULL to mean "never fetched".
        # So seed with first_seen=NULL.)
        conn.execute("""
            INSERT OR IGNORE INTO main (host, url, ref_host, ref_url, first_seen, last_seen, next_seen, response)
            VALUES (?, ?, NULL, NULL, NULL, NULL, NULL, NULL)
        """, (h, normalize_url_strip_query(start_url)))
        logging.info(f"Seeded: {start_url}")

seed_if_empty(STARTING_PAGE)

class LinkExtractor(HTMLParser):
    def __init__(self, base_url):
        super().__init__()
        self.base = base_url
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return
        href = None
        for k, v in attrs:
            if k.lower() == "href":
                href = v
                break
        if not href:
            return
        # Resolve relative URLs
        absu = urllib.parse.urljoin(self.base, href)
        self.links.append(absu)

def lease_batch(limit=MAX_BATCH):
    """
    Get up to `limit` URLs that need fetching:
    - first_seen IS NULL (never fetched) OR next_seen <= now()
    - host not under backoff (backoff=0 OR backoff_time <= now)
    Order by id (stable).
    """
    t = now_ts()
    sql = """
    SELECT m.id, m.url, m.host
    FROM main m
    JOIN hosts h ON h.host = m.host
    WHERE (m.first_seen IS NULL OR (m.next_seen IS NOT NULL AND m.next_seen <= ?))
      AND (h.backoff = 0 OR (h.backoff_time IS NOT NULL AND h.backoff_time <= ?))
    ORDER BY m.id
    LIMIT ?
    """
    return conn.execute(sql, (t, t, limit)).fetchall()

def save_body_to_disk(host: str, page_id: int, data: bytes):
    # sanitize host so it's filesystem safe
    safe_host = host.replace(":", "_").replace("/", "_")
    fn = os.path.join(PAGES_DIR, f"{safe_host}-{page_id}.txt")
    with open(fn, "wb") as f:
        f.write(data)

def rotate_archive_if_needed():
    # Count loose .txt files
    files = [f for f in os.listdir(PAGES_DIR) if f.endswith(".txt")]
    if len(files) < PAGES_PER_ARCHIVE:
        return
    # Name archive
    # Use an incrementing integer based on existing archives
    existing = [f for f in os.listdir(ARCHIVE_DIR) if f.endswith(".tar.gz")]
    idx = len(existing) + 1
    arc_name = os.path.join(ARCHIVE_DIR, f"pages_{idx:06d}.tar.gz")
    logging.info(f"Rotating archive -> {arc_name} with {len(files)} files")
    with tarfile.open(arc_name, "w:gz") as tar:
        for f in files:
            full = os.path.join(PAGES_DIR, f)
            tar.add(full, arcname=f)
    # After successful tar, delete loose files
    for f in files:
        try:
            os.remove(os.path.join(PAGES_DIR, f))
        except Exception as e:
            logging.error(f"Failed to remove {f} after archiving: {e}")

def body_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def transient_http(status: int) -> bool:
    """Identify statuses that merit a retry."""
    return status in (429, 500, 502, 503, 504)

def mark_host_backoff(host: str, seconds: int):
    t = now_ts() + seconds
    conn.execute("UPDATE hosts SET backoff=1, backoff_time=? WHERE host=?", (t, host))

def clear_host_backoff_if_expired(host: str):
    t = now_ts()
    conn.execute("""
        UPDATE hosts SET backoff=0, backoff_time=NULL
        WHERE host=? AND (backoff_time IS NULL OR backoff_time <= ?)
    """, (host, t))

def fetch_once(url: str):
    """
    Single attempt to fetch without following redirects programmatically.
    We rely on urllib to follow up to its default (but we will treat 3xx as 'ignore content').
    """
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            # Status code (Py <3.9 compatibility)
            status = getattr(resp, "status", 200)
            # Read up to MAX_BYTES
            data = resp.read(MAX_BYTES + 1)
            if len(data) > MAX_BYTES:
                data = data[:MAX_BYTES]
            return status, data
    except HTTPError as e:
        return e.code, b""
    except URLError as e:
        logging.warning(f"URLError for {url}: {e}")
        return -1, b""
    except Exception as e:
        logging.warning(f"Fetch exception for {url}: {e}")
        return -1, b""

def update_copyof_if_duplicate(page_id: int, rhash: str):
    row = conn.execute("""
        SELECT url FROM main WHERE response_hash = ? AND response = 200 AND id <> ?
        ORDER BY id LIMIT 1
    """, (rhash, page_id)).fetchone()
    if row:
        conn.execute("UPDATE main SET copyof=? WHERE id=?", (row[0], page_id))

def extract_and_enqueue_links(base_url: str, body_bytes: bytes):
    try:
        # Best-effort decode; we don't need perfect text, just <a href>
        text = body_bytes.decode(errors="ignore")
    except Exception:
        return
    parser = LinkExtractor(base_url)
    try:
        parser.feed(text)
    except Exception:
        pass
    new_count = 0
    for raw in parser.links:
        norm = normalize_url_strip_query(raw)
        if not norm:
            continue
        h = url_host(norm)
        if not h:
            continue
        ensure_host_row(h)
        try:
            conn.execute("""
                INSERT OR IGNORE INTO main (host, url, ref_host, ref_url, first_seen, last_seen, next_seen, response)
                VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL)
            """, (h, norm, url_host(base_url), base_url))
            new_count += 1
        except Exception:
            pass
    if new_count:
        logging.info(f"Enqueued {new_count} links from {base_url}")

def process_url(row):
    """
    row: (id, url, host)
    Implements your rules:
      - Ignore redirects (we'll record status if 3xx, but not follow)
      - 200 -> save, hash, set Next Seen = now + 14d
      - 404 -> Next Seen = now + 28d
      - 429 -> host backoff = +1h (remove from immediate queue by backoff)
      - 5xx -> host backoff = +15m
      - Others -> record status, modest next_seen (e.g., 7d)
    """
    page_id, url, host = row
    clear_host_backoff_if_expired(host)

    # Mark timestamps as we process
    first_seen_row = conn.execute("SELECT first_seen FROM main WHERE id=?", (page_id,)).fetchone()
    if first_seen_row and first_seen_row[0] is None:
        conn.execute("UPDATE main SET first_seen=? WHERE id=?", (now_ts(), page_id))

    # Try a few times on transient errors
    attempts = 0
    status = None
    data = b""
    while attempts < RETRY_LIMIT:
        attempts += 1
        status, data = fetch_once(url)
        if status == -1:
            # network error -> retry
            time.sleep(1.0 * attempts)
            continue
        if transient_http(status):
            # brief backoff before retry
            time.sleep(1.0 * attempts)
            # We won't follow redirects; if 429 or 5xx, we may also mark host backoff after loop.
        break

    t = now_ts()
    if 200 <= (status or 0) < 300:
        # Treat 2xx as success; store body, hash, set next_seen 14 days
        rhash = body_hash(data)
        save_body_to_disk(host, page_id, data)
        conn.execute("""
            UPDATE main
               SET last_seen=?, next_seen=?, response=?, response_hash=?
             WHERE id=?
        """, (t, t + 14*24*3600, status, rhash, page_id))
        update_copyof_if_duplicate(page_id, rhash)
        extract_and_enqueue_links(url, data)
        logging.info(f"{page_id} 200 OK {len(data)}B {url}")

    elif 300 <= (status or 0) < 400:
        # Redirects are ignored per spec; just record and move on; revisit in 7 days.
        conn.execute("""
            UPDATE main SET last_seen=?, next_seen=?, response=?, response_hash=NULL
            WHERE id=?
        """, (t, t + 7*24*3600, status, page_id))
        logging.info(f"{page_id} {status} redirect (ignored) {url}")

    elif status == 404:
        conn.execute("""
            UPDATE main SET last_seen=?, next_seen=?, response=?, response_hash=NULL
            WHERE id=?
        """, (t, t + 28*24*3600, status, page_id))
        logging.info(f"{page_id} 404 Not Found {url}")

    elif status == 429:
        # Host backoff +1h, leave URL to be retried later (via next_seen 1h)
        mark_host_backoff(host, BACKOFF_TIME)
        conn.execute("""
            UPDATE main SET last_seen=?, next_seen=?, response=?, response_hash=NULL
            WHERE id=?
        """, (t, t + BACKOFF_TIME, status, page_id))
        logging.info(f"{page_id} 429 rate-limited; host {host} backed off 1h {url}")

    elif status and 500 <= status < 600:
        # Host backoff +15m; modest revisit later (e.g., 1h)
        mark_host_backoff(host, SERVER_ERR_BACKOFF)
        conn.execute("""
            UPDATE main SET last_seen=?, next_seen=?, response=?, response_hash=NULL
            WHERE id=?
        """, (t, t + 3600, status, page_id))
        logging.info(f"{page_id} {status} server error; host {host} backed off 15m {url}")

    else:
        # Other cases (including network failures with status=-1)
        s = status if status is not None else -1
        conn.execute("""
            UPDATE main SET last_seen=?, next_seen=?, response=?, response_hash=NULL
            WHERE id=?
        """, (t, t + 7*24*3600, s, page_id))
        logging.info(f"{page_id} other status={s} {url}")

def main():
    logging.info("Crawler starting…")
    total_saved_before = len([f for f in os.listdir(PAGES_DIR) if f.endswith(".txt")])

    while True:
        batch = lease_batch(MAX_BATCH)
        if not batch:
            logging.info("No eligible URLs left. Exiting (revisit pass omitted by design).")
            rotate_archive_if_needed()
            break

        for row in batch:
            try:
                process_url(row)
            except Exception as e:
                # Defensive: never let one URL kill the loop
                logging.error(f"Error processing id={row[0]} url={row[1]}: {e}")

        rotate_archive_if_needed()

    total_saved_after = len([f for f in os.listdir(PAGES_DIR) if f.endswith(".txt")])
    logging.info(f"Crawler finished. Pages saved this run (loose): {total_saved_after - total_saved_before}")

if __name__ == "__main__":
    main()

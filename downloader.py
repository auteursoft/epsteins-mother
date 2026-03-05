#!/usr/bin/env python3
"""
downloader.py  (v3 – full folder enumeration via Drive API v3)

Usage:
    python downloader.py <url>
    python downloader.py --setup-key           # interactive API key setup

    # Override API key via env var:
    GDRIVE_API_KEY=AIza... python downloader.py <url>

Improvements over v2:
  - Uses Google Drive API v3 files.list with pagination (pageSize=1000)
    to enumerate ALL files in a folder, not just the first ~50
  - Recursively walks subfolders, preserving directory structure
  - Auto-extracts Firefox/Chrome cookies for drive.google.com
  - Downloads each file individually with retries + exponential backoff
  - One file's failure never stops the rest
  - Tracks progress and prints a summary at the end

Requirements:
    pip install requests beautifulsoup4 gdown browser-cookie3

    You also need a free Google API key with Drive API enabled.
    See: https://console.cloud.google.com/apis/credentials
"""

import sys
import os
import re
import json
import time
import http.cookiejar
import urllib.parse as urlparse
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

try:
    import gdown
    from gdown.exceptions import FileURLRetrievalError, FolderContentsMaximumLimitError
except ImportError:
    gdown = None

try:
    import browser_cookie3
except ImportError:
    browser_cookie3 = None


FAILED_LOG = Path("failed_downloads_google.txt")
GDOWN_COOKIE_PATH = Path.home() / ".cache" / "gdown" / "cookies.txt"
API_KEY_PATH = Path.home() / ".config" / "gdrive_downloader" / "api_key.txt"

# ── API key management ────────────────────────────────────────────────

def load_api_key() -> Optional[str]:
    """
    Load Google API key from (in priority order):
      1. GDRIVE_API_KEY environment variable
      2. ~/.config/gdrive_downloader/api_key.txt
    """
    key = os.environ.get("GDRIVE_API_KEY", "").strip()
    if key:
        return key

    if API_KEY_PATH.exists():
        key = API_KEY_PATH.read_text().strip()
        if key:
            return key

    return None


def save_api_key(key: str) -> None:
    """Save API key to ~/.config/gdrive_downloader/api_key.txt"""
    API_KEY_PATH.parent.mkdir(parents=True, exist_ok=True)
    API_KEY_PATH.write_text(key.strip() + "\n")
    API_KEY_PATH.chmod(0o600)
    print(f"[setup] API key saved to {API_KEY_PATH}")


def setup_key_interactive() -> None:
    """Interactive API key setup."""
    print("""
╔══════════════════════════════════════════════════════════════╗
║           Google Drive API Key Setup                        ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║  1. Go to https://console.cloud.google.com                   ║
║  2. Create a project (or pick an existing one)               ║
║  3. Go to "APIs & Services" > "Enabled APIs"                 ║
║     → Enable "Google Drive API"                              ║
║  4. Go to "APIs & Services" > "Credentials"                  ║
║     → Create Credentials > API Key                           ║
║  5. Copy the key and paste it below                          ║
║                                                              ║
║  This key is free and allows listing public folder contents. ║
║  No OAuth or consent screen needed.                          ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
""")
    key = input("Paste your API key: ").strip()
    if not key:
        print("No key entered, aborting.")
        sys.exit(1)

    # Quick validation
    test_url = f"https://www.googleapis.com/drive/v3/files?pageSize=1&key={key}"
    try:
        r = requests.get(test_url, timeout=10)
        if r.status_code == 200:
            print("[setup] API key is valid!")
        elif r.status_code == 403:
            data = r.json()
            msg = data.get("error", {}).get("message", "")
            if "not enabled" in msg.lower():
                print("[setup] WARNING: Key works but Google Drive API is not enabled.")
                print("        Go to APIs & Services > Enable 'Google Drive API'")
            else:
                print(f"[setup] WARNING: Got 403 – {msg}")
                print("        The key may still work for public files.")
        elif r.status_code == 400:
            print("[setup] WARNING: Key format may be invalid.")
        else:
            print(f"[setup] Got status {r.status_code} – saving anyway.")
    except Exception as e:
        print(f"[setup] Could not validate key ({e}) – saving anyway.")

    save_api_key(key)
    print("\nYou can now run: python downloader.py <google_drive_folder_url>")


# ── Cookie helpers ────────────────────────────────────────────────────

def _export_netscape_cookies(cj: http.cookiejar.CookieJar, path: Path) -> int:
    """Write a CookieJar to Netscape cookies.txt format. Returns count."""
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(path, "w") as f:
        f.write("# Netscape HTTP Cookie File\n")
        for c in cj:
            secure = "TRUE" if c.secure else "FALSE"
            domain_dot = "TRUE" if c.domain.startswith(".") else "FALSE"
            expires = str(c.expires) if c.expires else "0"
            f.write(f"{c.domain}\t{domain_dot}\t{c.path}\t{secure}\t{expires}\t{c.name}\t{c.value}\n")
            count += 1
    return count


def extract_browser_cookies(domain: str = ".google.com") -> Optional[http.cookiejar.CookieJar]:
    """Try to pull cookies from Firefox, then Chrome."""
    if browser_cookie3 is None:
        print("[cookies] browser_cookie3 not installed – pip install browser-cookie3")
        return None

    for browser_name, loader in [
        ("firefox", browser_cookie3.firefox),
        ("chrome", browser_cookie3.chrome),
    ]:
        try:
            cj = loader(domain_name=domain)
            if sum(1 for _ in cj) > 0:
                print(f"[cookies] Loaded cookies from {browser_name} for {domain}")
                return cj
        except Exception as e:
            print(f"[cookies] Could not load {browser_name} cookies: {e}")

    print("[cookies] No browser cookies found.")
    return None


def setup_cookies() -> Optional[http.cookiejar.CookieJar]:
    """Extract browser cookies and write to gdown's expected path."""
    cj = extract_browser_cookies()
    if cj is None:
        return None
    n = _export_netscape_cookies(cj, GDOWN_COOKIE_PATH)
    print(f"[cookies] Wrote {n} cookies to {GDOWN_COOKIE_PATH}")
    return cj


# ── Common helpers ────────────────────────────────────────────────────

def ensure_data_dir() -> Path:
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    return data_dir


def is_google_drive_url(url: str) -> bool:
    return "drive.google.com" in (urlparse.urlparse(url).netloc or "")


def is_dropbox_url(url: str) -> bool:
    return "dropbox.com" in (urlparse.urlparse(url).netloc or "")


def get_session(cookie_jar=None) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0 Safari/537.36"
        )
    })
    if cookie_jar is not None:
        s.cookies = cookie_jar
    return s


def safe_filename_from_url(url: str, fallback: str = "download") -> str:
    name = os.path.basename(urlparse.urlparse(url).path.rstrip("/"))
    return name or fallback


def log_failed_google_download(from_url, to_path, reason: str) -> None:
    url_str = from_url or "UNKNOWN_URL"
    path_str = str(to_path) if to_path is not None else "UNKNOWN_PATH"
    FAILED_LOG.parent.mkdir(parents=True, exist_ok=True)
    with FAILED_LOG.open("a", encoding="utf-8") as f:
        f.write(f"{url_str}\t{path_str}\t{reason}\n")


def guess_extension_from_content_type(ct: str) -> str:
    ct = ct.lower()
    for needle, ext in [
        ("image/jpeg", ".jpg"), ("image/png", ".png"), ("image/gif", ".gif"),
        ("application/pdf", ".pdf"), ("text/plain", ".txt"),
        ("image/tiff", ".tiff"), ("application/zip", ".zip"),
    ]:
        if needle in ct:
            return ext
    return ""


def _write_stream(response: requests.Response, path: Path) -> None:
    with open(path, "wb") as f:
        for chunk in response.iter_content(chunk_size=65536):
            if chunk:
                f.write(chunk)


def _extract_confirm_token(html: str) -> Optional[str]:
    m = re.search(r'confirm=([a-zA-Z0-9_-]+)', html)
    return m.group(1) if m else None


# ══════════════════════════════════════════════════════════════════════
# Google Drive API v3 folder enumeration (the key improvement)
# ══════════════════════════════════════════════════════════════════════

def extract_folder_id(url: str) -> Optional[str]:
    m = re.search(r'/folders/([a-zA-Z0-9_-]+)', url)
    return m.group(1) if m else None


def list_folder_files_api(
    folder_id: str,
    api_key: str,
    session: requests.Session,
) -> list[dict]:
    """
    Use Google Drive API v3 files.list to enumerate ALL files in a single
    folder. Handles pagination via nextPageToken. Returns list of dicts:
        [{"id": "...", "name": "...", "mimeType": "..."}, ...]
    """
    endpoint = "https://www.googleapis.com/drive/v3/files"
    # q = '<folder_id>' in parents AND not trashed
    query = f"'{folder_id}' in parents and trashed = false"
    fields = "nextPageToken, files(id, name, mimeType, size)"

    all_files = []
    page_token = None
    page_num = 0

    while True:
        page_num += 1
        params = {
            "q": query,
            "key": api_key,
            "pageSize": 1000,        # max allowed by the API
            "fields": fields,
            "orderBy": "name",
        }
        if page_token:
            params["pageToken"] = page_token

        r = session.get(endpoint, params=params, timeout=30)

        if r.status_code == 403:
            error_msg = r.json().get("error", {}).get("message", "")
            if "not enabled" in error_msg.lower():
                print("[api] ERROR: Google Drive API is not enabled for your project.")
                print("      Go to https://console.cloud.google.com/apis/library/drive.googleapis.com")
                print("      and click ENABLE.")
            elif "api key" in error_msg.lower():
                print(f"[api] ERROR: API key issue – {error_msg}")
            else:
                print(f"[api] ERROR 403: {error_msg}")
            return []

        if r.status_code == 404:
            print(f"[api] Folder {folder_id} not found or not public.")
            return []

        if r.status_code != 200:
            print(f"[api] Unexpected status {r.status_code}: {r.text[:300]}")
            return []

        data = r.json()
        files = data.get("files", [])
        all_files.extend(files)

        if page_num % 5 == 0 or not data.get("nextPageToken"):
            print(f"[api] Page {page_num}: {len(files)} items (total so far: {len(all_files)})")

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return all_files


def enumerate_folder_recursive(
    folder_id: str,
    api_key: str,
    session: requests.Session,
    path_prefix: str = "",
    depth: int = 0,
    max_depth: int = 20,
) -> list[dict]:
    """
    Recursively enumerate all files in a Google Drive folder tree.
    Returns a flat list of:
        [{"id": "...", "name": "...", "rel_path": "subfolder/file.jpg"}, ...]
    
    Subfolders are identified by mimeType == "application/vnd.google-apps.folder".
    """
    if depth > max_depth:
        print(f"[api] Max depth {max_depth} reached at {path_prefix}, stopping recursion.")
        return []

    indent = "  " * depth
    items = list_folder_files_api(folder_id, api_key, session)

    if not items:
        return []

    result = []
    subfolders = []

    for item in items:
        name = item["name"]
        mime = item.get("mimeType", "")
        rel = os.path.join(path_prefix, name) if path_prefix else name

        if mime == "application/vnd.google-apps.folder":
            subfolders.append((item["id"], name, rel))
        else:
            result.append({
                "id": item["id"],
                "name": name,
                "rel_path": rel,
                "mimeType": mime,
                "size": item.get("size"),
            })

    file_count = len(result)
    folder_count = len(subfolders)
    label = path_prefix or "(root)"
    print(f"{indent}[api] {label}: {file_count} files, {folder_count} subfolders")

    # Recurse into subfolders
    for sf_id, sf_name, sf_path in subfolders:
        sub_files = enumerate_folder_recursive(
            sf_id, api_key, session,
            path_prefix=sf_path,
            depth=depth + 1,
            max_depth=max_depth,
        )
        result.extend(sub_files)

    return result


# ── Fallback: HTML scrape for when there's no API key ────────────────

def enumerate_folder_html_scrape(
    folder_url: str, session: requests.Session
) -> list[dict]:
    """
    Scrape the Google Drive folder HTML page. Only gets the first ~50 files.
    Used as a last-resort fallback when no API key is available.
    """
    folder_id = extract_folder_id(folder_url)
    if not folder_id:
        return []

    try:
        page_url = f"https://drive.google.com/drive/folders/{folder_id}"
        print(f"[scrape] Fetching folder page: {page_url}")
        r = session.get(page_url, timeout=60)
        r.raise_for_status()

        pattern = re.compile(
            r'\["(1[a-zA-Z0-9_-]{10,})"\s*,\s*"([^"]+\.[a-zA-Z0-9]{1,10})"'
        )
        seen = set()
        files = []
        for match in pattern.finditer(r.text):
            fid, fname = match.group(1), match.group(2)
            if fid not in seen:
                seen.add(fid)
                files.append({"id": fid, "name": fname, "rel_path": fname})

        if files:
            print(f"[scrape] Found {len(files)} files (may be incomplete for large folders)")
        return files

    except Exception as e:
        print(f"[scrape] HTML scrape failed: {e}")
        return []


# ── Per-file Google Drive download with retries ──────────────────────

def download_gdrive_file(
    file_id: str,
    rel_path: str,
    out_dir: Path,
    session: requests.Session,
    max_retries: int = 4,
    base_delay: float = 5.0,
) -> bool:
    """
    Download a single Google Drive file by ID, with exponential backoff.
    rel_path is the relative path (including subfolder structure) under out_dir.
    Returns True on success.
    """
    out_path = out_dir / rel_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and out_path.stat().st_size > 0:
        print(f"  [skip] Already exists: {out_path}")
        return True

    url = f"https://drive.google.com/uc?id={file_id}"

    for attempt in range(1, max_retries + 1):
        delay = base_delay * (2 ** (attempt - 1))

        # ── Strategy 1: gdown (attempts 1-2 only) ──
        if gdown is not None and attempt <= 2:
            try:
                result = gdown.download(
                    url=url,
                    output=str(out_path),
                    quiet=True,
                    fuzzy=False,
                    use_cookies=True,
                )
                if result and out_path.exists() and out_path.stat().st_size > 0:
                    return True
            except FileURLRetrievalError:
                pass  # fall through to direct GET
            except Exception:
                pass

        # ── Strategy 2: direct GET with cookies + confirm token ──
        try:
            direct_url = f"https://drive.google.com/uc?id={file_id}&export=download&confirm=t"
            with session.get(direct_url, stream=True, timeout=300) as r:
                ct = r.headers.get("Content-Type", "").lower()

                if "text/html" in ct and r.status_code == 200:
                    confirm_token = _extract_confirm_token(r.text)
                    if confirm_token:
                        confirmed_url = f"{direct_url}&confirm={confirm_token}"
                        with session.get(confirmed_url, stream=True, timeout=300) as r2:
                            r2.raise_for_status()
                            if "text/html" not in r2.headers.get("Content-Type", "").lower():
                                _write_stream(r2, out_path)
                                return True

                    # Still HTML → rate limited
                    if attempt < max_retries:
                        print(f"  [wait] Rate limited, backing off {delay:.0f}s ...")
                        time.sleep(delay)
                        continue
                    else:
                        break  # give up

                r.raise_for_status()
                _write_stream(r, out_path)
                return True

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                print(f"  [wait] 429 rate limit, backing off {delay:.0f}s ...")
                time.sleep(delay)
                continue
            print(f"  [err] HTTP error: {e}")
        except Exception as e:
            print(f"  [err] {e}")

        if attempt < max_retries:
            time.sleep(delay)

    reason = f"All {max_retries} attempts failed"
    log_failed_google_download(url, out_path, reason)
    return False


# ── Main Google Drive entry point ────────────────────────────────────

def download_google_drive(url: str, out_dir: Path, cookie_jar=None) -> None:
    """
    Robust Google Drive downloader.

    For folders:
      1. Enumerate ALL files via Drive API v3 (with pagination + recursion)
      2. Fall back to HTML scrape if no API key
      3. Download each file individually with retries
      4. Never let one failure stop the rest

    For single files:
      Uses the same per-file retry logic.
    """
    parsed = urlparse.urlparse(url)
    path = parsed.path or ""
    session = get_session(cookie_jar)
    api_key = load_api_key()

    print(f"[gdrive] Processing: {url}")

    # ── Single file ──
    if "/folders/" not in path and "/drive/folders/" not in path:
        m = re.search(r'/d/([a-zA-Z0-9_-]+)', path)
        if m:
            file_id = m.group(1)
        else:
            qs = dict(urlparse.parse_qsl(parsed.query))
            file_id = qs.get("id", "")
        if not file_id:
            print(f"[gdrive] Could not extract file ID from {url}")
            return
        download_gdrive_file(file_id, file_id, out_dir, session)
        return

    # ── Folder ──
    folder_id = extract_folder_id(url)
    if not folder_id:
        print(f"[gdrive] Could not extract folder ID from {url}")
        return

    # Enumerate files
    files = []
    if api_key:
        print(f"[gdrive] Using Drive API v3 to enumerate folder (full pagination)...")
        files = enumerate_folder_recursive(folder_id, api_key, session)
    else:
        print("[gdrive] ┌─────────────────────────────────────────────────────────┐")
        print("[gdrive] │ No API key found. Large folders will be INCOMPLETE.     │")
        print("[gdrive] │ Run: python downloader.py --setup-key                   │")
        print("[gdrive] │ Or set GDRIVE_API_KEY=... in your environment.          │")
        print("[gdrive] └─────────────────────────────────────────────────────────┘")

    # If API enumeration returned nothing, try gdown first, then HTML scrape
    if not files:
        if gdown is not None:
            try:
                print("[gdrive] Trying gdown for folder download...")
                gdown.download_folder(
                    url=url,
                    output=str(out_dir),
                    quiet=False,
                    use_cookies=True,
                    remaining_ok=True,
                    resume=True,
                )
                print("[gdrive] gdown folder download completed.")
                return
            except Exception as e:
                print(f"[gdrive] gdown folder download failed: {e}")

        print("[gdrive] Falling back to HTML scrape (will be incomplete for large folders)...")
        files = enumerate_folder_html_scrape(url, session)

    if not files:
        reason = "Could not enumerate any files in folder"
        print(f"[gdrive] {reason}")
        log_failed_google_download(url, out_dir, reason)
        return

    # Download all files
    total = len(files)
    print(f"\n[gdrive] ═══ Downloading {total} files ═══\n")

    success = 0
    skipped = 0
    failed = 0
    start_time = time.time()

    for i, finfo in enumerate(files, 1):
        fid = finfo["id"]
        rel_path = finfo["rel_path"]

        # Check if already downloaded
        target = out_dir / rel_path
        if target.exists() and target.stat().st_size > 0:
            skipped += 1
            if i % 50 == 0:
                print(f"  [{i}/{total}] (skipping already-downloaded files...)")
            continue

        print(f"  [{i}/{total}] {rel_path}")
        ok = download_gdrive_file(fid, rel_path, out_dir, session)
        if ok:
            success += 1
        else:
            failed += 1

        # Polite delay to avoid rate limiting (smaller for small files)
        if i < total:
            time.sleep(0.8)

    elapsed = time.time() - start_time
    print(f"\n[gdrive] ═══ Folder complete ═══")
    print(f"[gdrive]   Total files:  {total}")
    print(f"[gdrive]   Downloaded:   {success}")
    print(f"[gdrive]   Skipped:      {skipped}")
    print(f"[gdrive]   Failed:       {failed}")
    print(f"[gdrive]   Elapsed:      {elapsed:.1f}s")
    if failed > 0:
        print(f"[gdrive]   See {FAILED_LOG} for details on failures.")


# ── Dropbox ──────────────────────────────────────────────────────────

def download_dropbox_file(url: str, out_dir: Path) -> None:
    parsed = urlparse.urlparse(url)
    query = dict(urlparse.parse_qsl(parsed.query))
    query["dl"] = "1"
    direct_url = urlparse.urlunparse(parsed._replace(query=urlparse.urlencode(query)))

    session = get_session()
    print(f"[dropbox] Downloading {direct_url}")

    with session.get(direct_url, stream=True, timeout=300) as r:
        r.raise_for_status()
        cd = r.headers.get("Content-Disposition", "")
        m = re.search(r'filename\*?="?([^";]+)"?', cd)
        filename = m.group(1) if m else safe_filename_from_url(parsed.path, "dropbox_download")

        local_path = out_dir / filename
        local_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"[dropbox] -> {local_path}")
        _write_stream(r, local_path)


# ── Generic + page scraping ──────────────────────────────────────────

def make_local_path_for_generic(url: str, base_dir: Path) -> Path:
    parsed = urlparse.urlparse(url)
    netloc = parsed.netloc.replace(":", "_")
    path = parsed.path.lstrip("/") or "index"
    if path.endswith("/"):
        path += "index"
    return base_dir / Path(netloc) / Path(path)


def download_generic_file(url: str, out_dir: Path) -> None:
    session = get_session()
    local_path = make_local_path_for_generic(url, out_dir)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"[generic] Downloading {url} -> {local_path}")
    with session.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        _write_stream(r, local_path)


def find_links_on_page(page_url: str):
    session = get_session()
    print(f"[page] Fetching {page_url}")
    r = session.get(page_url, timeout=120)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a", href=True):
        yield urlparse.urljoin(r.url, a["href"])


def looks_like_direct_file(url: str) -> bool:
    path = urlparse.urlparse(url).path.lower()
    return path.endswith((".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".zip", ".txt"))


# ── Main ─────────────────────────────────────────────────────────────

def process_url(url: str) -> None:
    data_dir = ensure_data_dir()
    cookie_jar = setup_cookies()

    if is_google_drive_url(url):
        download_google_drive(url, data_dir, cookie_jar)
        return

    if is_dropbox_url(url):
        download_dropbox_file(url, data_dir)
        return

    # HTML page — scrape for links
    seen = set()
    for link in find_links_on_page(url):
        if link in seen:
            continue
        seen.add(link)

        if is_google_drive_url(link):
            try:
                download_google_drive(link, data_dir, cookie_jar)
            except Exception as e:
                print(f"[gdrive] Failed on {link}: {e}")
        elif is_dropbox_url(link):
            try:
                download_dropbox_file(link, data_dir)
            except Exception as e:
                print(f"[dropbox] Failed on {link}: {e}")
        elif looks_like_direct_file(link):
            try:
                download_generic_file(link, data_dir)
            except Exception as e:
                print(f"[generic] Failed on {link}: {e}")


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "--setup-key":
        setup_key_interactive()
        sys.exit(0)

    if len(sys.argv) != 2:
        print("Usage: python downloader.py <url>")
        print("       python downloader.py --setup-key")
        sys.exit(1)

    process_url(sys.argv[1])

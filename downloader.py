#!/usr/bin/env python3
"""
downloader.py  (v2 – resilient Google Drive folder downloads)

Usage:
    python downloader.py <url>

Improvements over v1:
  - Auto-extracts Firefox (or Chrome) cookies for drive.google.com
  - Enumerates Google Drive folder contents by scraping the folder page
  - Downloads each file individually with retries + exponential backoff
  - One file's failure never stops the rest of the folder
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
    """
    Try to pull cookies from Firefox, then Chrome, for the given domain.
    Returns a CookieJar or None.
    """
    if browser_cookie3 is None:
        print("[cookies] browser_cookie3 not installed – pip install browser-cookie3")
        return None

    for browser_name, loader in [
        ("firefox", browser_cookie3.firefox),
        ("chrome", browser_cookie3.chrome),
    ]:
        try:
            cj = loader(domain_name=domain)
            # Quick sanity: does it have any cookies?
            if sum(1 for _ in cj) > 0:
                print(f"[cookies] Loaded cookies from {browser_name} for {domain}")
                return cj
        except Exception as e:
            print(f"[cookies] Could not load {browser_name} cookies: {e}")

    print("[cookies] No browser cookies found.")
    return None


def setup_cookies() -> Optional[http.cookiejar.CookieJar]:
    """
    Extract browser cookies and:
      1. Write them to ~/.cache/gdown/cookies.txt  (so gdown picks them up)
      2. Return the CookieJar for use with requests
    """
    cj = extract_browser_cookies()
    if cj is None:
        return None

    n = _export_netscape_cookies(cj, GDOWN_COOKIE_PATH)
    print(f"[cookies] Wrote {n} cookies to {GDOWN_COOKIE_PATH}")
    return cj


# ── Common helpers (unchanged from v1) ────────────────────────────────

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
    parsed = urlparse.urlparse(url)
    name = os.path.basename(parsed.path.rstrip("/"))
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


# ── Google Drive folder enumeration ──────────────────────────────────

def extract_folder_id(url: str) -> Optional[str]:
    """Pull the folder ID out of a Google Drive folder URL."""
    m = re.search(r"/folders/([a-zA-Z0-9_-]+)", url)
    return m.group(1) if m else None


def enumerate_folder_files(folder_url: str, session: requests.Session) -> list[dict]:
    """
    Scrape a public Google Drive folder page to get a list of
    {id, name} dicts for every file. Works without an API key by
    parsing the JS data blob Google embeds in the HTML.
    
    Falls back to gdown's internal listing if scraping fails.
    """
    folder_id = extract_folder_id(folder_url)
    if not folder_id:
        return []

    files = []

    # Strategy 1: Use the undocumented but stable JSON endpoint that
    # Google's folder viewer hits. This returns file metadata as JSON.
    # The "export" link works like:
    #   https://drive.google.com/drive/folders/<id>  (the HTML page)
    # The HTML embeds a big JS array we can parse.
    try:
        page_url = f"https://drive.google.com/drive/folders/{folder_id}"
        print(f"[enum] Fetching folder page: {page_url}")
        r = session.get(page_url, timeout=60)
        r.raise_for_status()
        html = r.text

        # Google embeds file data in a JS variable. The pattern looks like:
        #   window['_DRIVE_ivd'] = '...escaped JSON...';
        # But more reliably, each file appears as an array element with
        # the pattern: ["<file_id>","<filename>", ...
        # We look for all file-ID-sized strings followed by filenames.
        # 
        # More robust: find all entries that look like
        #   ["1D6O3V4HB_M19Czv4Ngv6LYZV1tWRPfb7","HOUSE_OVERSIGHT_018484.jpg"
        pattern = re.compile(
            r'\["(1[a-zA-Z0-9_-]{10,})"\s*,\s*"([^"]+\.[a-zA-Z0-9]{1,10})"'
        )
        for match in pattern.finditer(html):
            fid, fname = match.group(1), match.group(2)
            files.append({"id": fid, "name": fname})

        # Deduplicate by ID
        seen = set()
        deduped = []
        for f in files:
            if f["id"] not in seen:
                seen.add(f["id"])
                deduped.append(f)
        files = deduped

        if files:
            print(f"[enum] Found {len(files)} files by scraping folder HTML")
            return files

    except Exception as e:
        print(f"[enum] HTML scrape failed: {e}")

    # Strategy 2: Use gdown's internal folder listing (it parses the
    # same page but may fail on large folders)
    if gdown is not None:
        try:
            print("[enum] Trying gdown._parse_google_drive_folder ...")
            # gdown >= 5.x exposes this; exact API varies by version
            from gdown.download_folder import _parse_google_drive_folder_url
            return_code, gdrive_files = _parse_google_drive_folder_url(folder_url)
            if gdrive_files:
                files = [{"id": f.id, "name": f.name} for f in gdrive_files]
                print(f"[enum] gdown listed {len(files)} files")
                return files
        except Exception as e:
            print(f"[enum] gdown internal listing failed: {e}")

    print("[enum] Could not enumerate folder contents.")
    return []


# ── Per-file Google Drive download with retries ──────────────────────

def download_gdrive_file(
    file_id: str,
    filename: str,
    out_dir: Path,
    session: requests.Session,
    max_retries: int = 4,
    base_delay: float = 5.0,
) -> bool:
    """
    Download a single Google Drive file by ID, with exponential backoff.
    
    Tries three strategies in order:
      1. gdown.download (benefits from cookies in ~/.cache/gdown/)
      2. Direct GET to /uc?id=...&export=download&confirm=t  (with session cookies)
      3. Confirm-token scrape for large files
    
    Returns True on success.
    """
    out_path = out_dir / filename
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and out_path.stat().st_size > 0:
        print(f"[skip] Already exists: {out_path}")
        return True

    url = f"https://drive.google.com/uc?id={file_id}"

    for attempt in range(1, max_retries + 1):
        delay = base_delay * (2 ** (attempt - 1))

        # ── Try gdown first ──
        if gdown is not None and attempt <= 2:
            try:
                print(f"[gdrive] ({attempt}/{max_retries}) gdown: {filename}")
                result = gdown.download(
                    url=url,
                    output=str(out_path),
                    quiet=False,
                    fuzzy=False,
                    use_cookies=True,
                )
                if result and out_path.exists() and out_path.stat().st_size > 0:
                    return True
            except FileURLRetrievalError:
                print(f"[gdrive] gdown FileURLRetrievalError on {filename}, will try direct GET")
            except Exception as e:
                print(f"[gdrive] gdown error on {filename}: {e}")

        # ── Direct GET with cookies ──
        try:
            direct_url = f"https://drive.google.com/uc?id={file_id}&export=download&confirm=t"
            print(f"[gdrive] ({attempt}/{max_retries}) direct GET: {filename}")
            with session.get(direct_url, stream=True, timeout=300) as r:
                ct = r.headers.get("Content-Type", "").lower()

                # If we get an HTML page, it's the "virus scan" confirmation page
                if "text/html" in ct and r.status_code == 200:
                    # Try to extract the confirm token
                    confirm_token = _extract_confirm_token(r.text)
                    if confirm_token:
                        confirmed_url = f"{direct_url}&confirm={confirm_token}"
                        print(f"[gdrive] Retrying with confirm token for {filename}")
                        with session.get(confirmed_url, stream=True, timeout=300) as r2:
                            r2.raise_for_status()
                            ct2 = r2.headers.get("Content-Type", "").lower()
                            if "text/html" not in ct2:
                                _write_stream(r2, out_path)
                                return True
                    # Still HTML – rate limited or permission denied
                    print(f"[gdrive] Got HTML response for {filename}, backing off {delay:.0f}s")
                    time.sleep(delay)
                    continue

                r.raise_for_status()
                _write_stream(r, out_path)
                return True

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                print(f"[gdrive] Rate limited on {filename}, backing off {delay:.0f}s")
                time.sleep(delay)
                continue
            print(f"[gdrive] HTTP error on {filename}: {e}")
        except Exception as e:
            print(f"[gdrive] Error on {filename}: {e}")

        time.sleep(delay)

    # Exhausted retries
    reason = f"All {max_retries} attempts failed for {filename}"
    print(f"[gdrive] FAILED: {reason}")
    log_failed_google_download(url, out_path, reason)
    return False


def _extract_confirm_token(html: str) -> Optional[str]:
    """Pull the download confirm token from a Google Drive warning page."""
    # Pattern 1: form with confirm=...
    m = re.search(r'confirm=([a-zA-Z0-9_-]+)', html)
    if m:
        return m.group(1)
    # Pattern 2: /uc?...&confirm=...
    m = re.search(r'id="download-form".*?confirm=([^&"]+)', html, re.DOTALL)
    if m:
        return m.group(1)
    return None


def _write_stream(response: requests.Response, path: Path) -> None:
    """Stream response body to a file."""
    with open(path, "wb") as f:
        for chunk in response.iter_content(chunk_size=65536):
            if chunk:
                f.write(chunk)


# ── Main Google Drive entry point ────────────────────────────────────

def download_google_drive(url: str, out_dir: Path, cookie_jar=None) -> None:
    """
    Robust Google Drive downloader.
    
    For folders:
      1. Enumerate all files in the folder (scrape + fallback to gdown internals)
      2. Download each file individually with retries
      3. Never let one failure stop the rest
    
    For single files:
      Uses the same per-file retry logic.
    """
    parsed = urlparse.urlparse(url)
    path = parsed.path or ""

    session = get_session(cookie_jar)
    print(f"[gdrive] Processing: {url}")

    # ── Single file ──
    if "/folders/" not in path and "/drive/folders/" not in path:
        # Extract file ID
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

    # First, try gdown for the whole folder (it handles subfolder
    # structure nicely). If it succeeds fully, great.
    if gdown is not None:
        try:
            print("[gdrive] Attempting full gdown folder download first...")
            gdown.download_folder(
                url=url,
                output=str(out_dir),
                quiet=False,
                use_cookies=True,
                remaining_ok=True,
                resume=True,
            )
            print("[gdrive] gdown folder download completed successfully.")
            return
        except (FileURLRetrievalError, FolderContentsMaximumLimitError, json.JSONDecodeError) as e:
            print(f"[gdrive] gdown folder download hit error: {e}")
            print("[gdrive] Falling back to per-file enumeration + download...")
        except Exception as e:
            print(f"[gdrive] gdown unexpected error: {e}")
            print("[gdrive] Falling back to per-file enumeration + download...")

    # Enumerate files in the folder
    files = enumerate_folder_files(url, session)
    if not files:
        reason = "Could not enumerate any files in folder"
        print(f"[gdrive] {reason}")
        log_failed_google_download(url, out_dir, reason)
        return

    print(f"[gdrive] Downloading {len(files)} files individually...")
    success = 0
    failed = 0
    for i, finfo in enumerate(files, 1):
        fid = finfo["id"]
        fname = finfo["name"]
        print(f"\n[gdrive] [{i}/{len(files)}] {fname}")

        ok = download_gdrive_file(fid, fname, out_dir, session)
        if ok:
            success += 1
        else:
            failed += 1

        # Small polite delay between files to avoid triggering rate limits
        if i < len(files):
            time.sleep(1.5)

    print(f"\n[gdrive] Folder done: {success} succeeded, {failed} failed out of {len(files)}")


# ── Dropbox (unchanged) ──────────────────────────────────────────────

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


# ── Generic file + page scraping (unchanged) ─────────────────────────

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

    # Set up browser cookies before anything else
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
    if len(sys.argv) != 2:
        print("Usage: python downloader.py <url>")
        sys.exit(1)

    process_url(sys.argv[1])
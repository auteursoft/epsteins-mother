#!/usr/bin/env python3
"""
downloader.py

Usage:
    python downloader.py <url>

Given either:
- a web page containing Google Drive / Dropbox links (e.g. the Oversight press release), OR
- a direct Google Drive or Dropbox link,

it downloads all referenced assets into ./data, preserving folder structure where possible.
"""

import sys
import os
import re
import urllib.parse as urlparse
from pathlib import Path
import json
import requests
from bs4 import BeautifulSoup

try:
    import gdown
except ImportError:
    gdown = None
import requests
import gdown
from gdown.exceptions import FileURLRetrievalError, FolderContentsMaximumLimitError

FAILED_LOG = Path("failed_downloads_google.txt")

# ----------------- Helpers -----------------

def ensure_data_dir() -> Path:
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    return data_dir


def is_google_drive_url(url: str) -> bool:
    parsed = urlparse.urlparse(url)
    return "drive.google.com" in (parsed.netloc or "")


def is_dropbox_url(url: str) -> bool:
    parsed = urlparse.urlparse(url)
    return "dropbox.com" in (parsed.netloc or "")


def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/129.0 Safari/537.36"
        )
    })
    return s


def safe_filename_from_url(url: str, fallback: str = "download") -> str:
    parsed = urlparse.urlparse(url)
    path = parsed.path
    name = os.path.basename(path.rstrip("/"))
    return name or fallback


def make_local_path_for_generic(url: str, base_dir: Path) -> Path:
    """
    Create a local path that roughly mirrors the domain + path structure.
    e.g. https://example.com/docs/a/b.pdf -> data/example.com/docs/a/b.pdf
    """
    parsed = urlparse.urlparse(url)
    netloc = parsed.netloc.replace(":", "_")
    path = parsed.path.lstrip("/") or "index"
    # Make sure we have some filename at the end
    if path.endswith("/"):
        path += "index"
    rel = Path(netloc) / Path(path)
    return base_dir / rel


def download_generic_file(url: str, out_dir: Path) -> None:
    """
    Download any non-GDrive/non-Dropbox file, saving under out_dir
    mirroring domain/path structure.
    """
    session = get_session()
    local_path = make_local_path_for_generic(url, out_dir)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"[generic] Downloading {url} -> {local_path}")
    with session.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)


def download_dropbox_file(url: str, out_dir: Path) -> None:
    """
    Download a Dropbox shared link.
    If it's a typical ?dl=0 link, switch to ?dl=1 for direct download.
    """
    parsed = urlparse.urlparse(url)
    query = dict(urlparse.parse_qsl(parsed.query))
    # Force direct download
    query["dl"] = "1"

    direct_url = urlparse.urlunparse(
        parsed._replace(query=urlparse.urlencode(query))
    )

    session = get_session()
    print(f"[dropbox] Downloading {direct_url}")

    with session.get(direct_url, stream=True, timeout=300) as r:
        r.raise_for_status()

        cd = r.headers.get("Content-Disposition", "")
        filename_match = re.search(r'filename\*?="?([^";]+)"?', cd)
        if filename_match:
            filename = filename_match.group(1)
        else:
            filename = safe_filename_from_url(parsed.path, "dropbox_download")

        local_path = out_dir / filename
        local_path.parent.mkdir(parents=True, exist_ok=True)

        print(f"[dropbox] -> {local_path}")
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

# Log lives in the repo root; change to Path("data") / "failed_downloads_google.txt" if you prefer.
FAILED_LOG = Path("failed_downloads_google.txt")


def log_failed_google_download(from_url: str | None, to_path: Path | None, reason: str) -> None:
    """
    Append a line to failed_downloads_google.txt with:
        URL<TAB>LOCAL_PATH<TAB>REASON
    """
    url_str = from_url or "UNKNOWN_URL"
    path_str = str(to_path) if to_path is not None else "UNKNOWN_PATH"
    line = f"{url_str}\t{path_str}\t{reason}\n"
    FAILED_LOG.parent.mkdir(parents=True, exist_ok=True)
    with FAILED_LOG.open("a", encoding="utf-8") as f:
        f.write(line)


def guess_extension_from_content_type(content_type: str) -> str:
    ct = content_type.lower()
    if "image/jpeg" in ct:
        return ".jpg"   # fine for .jpeg as well
    if "image/png" in ct:
        return ".png"
    if "image/gif" in ct:
        return ".gif"
    if "application/pdf" in ct:
        return ".pdf"
    if "text/plain" in ct:
        return ".txt"
    return ""


def download_file_direct_to_path(from_url: str, to_path: Path) -> None:
    """
    Fallback: download a Google Drive 'uc?id=...' URL directly to an explicit path.
    """
    to_path = Path(to_path)
    to_path.parent.mkdir(parents=True, exist_ok=True)

    parsed = urlparse.urlparse(from_url)
    query = dict(urlparse.parse_qsl(parsed.query))
    query.setdefault("export", "download")
    direct_url = urlparse.urlunparse(
        parsed._replace(query=urlparse.urlencode(query))
    )

    print(f"[fallback] Direct GET for {direct_url}")
    with requests.get(direct_url, stream=True, timeout=120) as r:
        r.raise_for_status()
        content_type = r.headers.get("Content-Type", "").lower()

        if "text/html" in content_type:
            reason = f"HTML response (Content-Type={content_type}) when writing to {to_path}"
            print("[fallback] WARNING:", reason)
            log_failed_google_download(direct_url, to_path, reason)

        print(f"[fallback] -> {to_path}")
        with open(to_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)


def download_file_direct_guess_name(from_url: str, out_dir: Path) -> Path:
    """
    Fallback when we *don't* know the intended filename/path.
    We:
    - GET the URL
    - try Content-Disposition filename
    - else use the id=... + extension guessed from Content-Type
    - save under out_dir / 'gdrive_fallback' / filename
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(exist_ok=True, parents=True)

    parsed = urlparse.urlparse(from_url)
    query = dict(urlparse.parse_qsl(parsed.query))
    file_id = query.get("id", "unknown_id")

    query.setdefault("export", "download")
    direct_url = urlparse.urlunparse(
        parsed._replace(query=urlparse.urlencode(query))
    )

    print(f"[fallback] Direct GET (guess name) for {direct_url}")
    with requests.get(direct_url, stream=True, timeout=120) as r:
        r.raise_for_status()
        content_type = r.headers.get("Content-Type", "").lower()
        cd = r.headers.get("Content-Disposition", "")

        filename = None
        m = re.search(r'filename\*?="?([^";]+)"?', cd)
        if m:
            filename = m.group(1)

        if not filename:
            ext = guess_extension_from_content_type(content_type)
            filename = file_id + ext

        fallback_dir = out_dir / "gdrive_fallback"
        fallback_dir.mkdir(parents=True, exist_ok=True)
        to_path = fallback_dir / filename

        if "text/html" in content_type:
            reason = f"HTML response (Content-Type={content_type}) when guessing name {to_path}"
            print("[fallback] WARNING:", reason)
            log_failed_google_download(direct_url, to_path, reason)

        print(f"[fallback] -> {to_path}")
        with open(to_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    return to_path


def download_google_drive(url: str, out_dir: Path) -> None:
    """
    Use gdown to download from Google Drive.

    - If it's a folder, we call gdown.download_folder in a loop.
    On each FileURLRetrievalError, we:
        * parse the offending uc?id=... URL from the error message
        * download it ourselves with requests
        * then retry gdown with resume=True until no more errors occur
    - If it's a single file, we just use gdown.download once.

    Any files we *still* can't grab are logged to failed_downloads_google.txt.
    """
    if gdown is None:
        raise RuntimeError(
            "gdown is not installed. Install it with: pip install gdown"
        )

    parsed = urlparse.urlparse(url)
    path = parsed.path or ""

    print(f"[gdrive] Downloading from Google Drive: {url}")

    # Single-file case: use gdown directly
    if "/folders/" not in path and "/drive/folders/" not in path:
        try:
            gdown.download(
                url=url,
                output=str(out_dir),
                quiet=False,
                fuzzy=True,
            )
        except FileURLRetrievalError as e:
            msg = str(e)
            print("[gdrive] FileURLRetrievalError on single file, trying fallback.")
            print(msg.strip())

            browser_url_match = re.search(
                r"https?://drive\.google\.com/uc\?[^ \n]+", msg
            )
            if browser_url_match:
                from_url = browser_url_match.group(0).strip()
                try:
                    written_path = download_file_direct_guess_name(from_url, out_dir)
                    print(f"[gdrive] Fallback single-file download finished: {written_path}")
                except Exception as inner_e:
                    reason = f"Fallback single-file exception: {inner_e}"
                    print(f"[gdrive] Fallback single-file FAILED: {inner_e}")
                    log_failed_google_download(from_url, None, reason)
            else:
                reason = "Could not parse uc?id=... URL from single-file FileURLRetrievalError"
                print("[gdrive] " + reason)
                log_failed_google_download(None, None, reason)
        return

    # Folder case: keep calling gdown until it completes or we hit a hard failure
    seen_bad_ids: set[str] = set()

    # Folder case: single gdown call with one-shot fallback on error
    # Folder case: retry a few times on JSONDecodeError; one-shot fallback on FileURLRetrievalError
    max_retries = 3

    for attempt in range(1, max_retries + 1):
        try:
            print(f"[gdrive] Folder download attempt {attempt}/{max_retries}")
            gdown.download_folder(
                url=url,
                output=str(out_dir),
                quiet=False,
                use_cookies=False,
                remaining_ok=True,
                resume=True,
            )
            print("[gdrive] Folder download completed with gdown.")
            return  # success, we're done with this folder

        except FolderContentsMaximumLimitError as e:
            print(f"[gdrive] Folder limit error from gdown: {e}")
            print("[gdrive] Try upgrading gdown: pip install --upgrade gdown")
            log_failed_google_download(url, out_dir, f"FolderContentsMaximumLimitError: {e}")
            return  # can't do much more automatically

        except FileURLRetrievalError as e:
            msg = str(e)
            print("[gdrive] FileURLRetrievalError encountered during folder download, trying fallback.")
            print(msg.strip())

            # Try to extract uc?id=... URL from the message
            browser_url_match = re.search(
                r"https?://drive\.google\.com/uc\?[^ \n]+", msg
            )
            if not browser_url_match:
                reason = "Could not parse uc?id=... URL from FileURLRetrievalError"
                print("[gdrive] " + reason)
                log_failed_google_download(None, None, reason)
                return

            from_url = browser_url_match.group(0).strip()

            # Try to grab more precise "To:" path if gdown reported it (older-style messages)
            to_match = re.search(r"To:\s*([^\n]+)", msg)
            try:
                if to_match:
                    to_path = Path(to_match.group(1).strip())
                    download_file_direct_to_path(from_url, to_path)
                    print(f"[gdrive] Fallback direct download (explicit path) finished: {to_path}")
                else:
                    written_path = download_file_direct_guess_name(from_url, out_dir)
                    print(f"[gdrive] Fallback direct download (guessed name) finished: {written_path}")
                # After fallback, we don't re-run gdown for this folder; it will just fail on the same item.
                print("[gdrive] Stopping after fallback; some later files in this folder may not be downloaded by gdown.")
            except Exception as inner_e:
                reason = f"Fallback direct download exception: {inner_e}"
                print(f"[gdrive] Fallback direct download FAILED: {inner_e}")
                log_failed_google_download(from_url, None, reason)

            return  # we're done with this folder, successful or not

        except json.JSONDecodeError as e:
            # gdown expected JSON from Google but got empty/HTML/etc.
            reason = f"JSONDecodeError in gdown.download_folder: {e}"
            print("[gdrive] " + reason)
            if attempt == max_retries:
                # After several tries, give up on this folder
                log_failed_google_download(url, out_dir, reason)
                return
            else:
                print("[gdrive] Retrying after JSONDecodeError...")
                continue  # try gdown.download_folder again

        except Exception as e:
            # Catch-all for any other unexpected gdown internals
            reason = f"Unexpected error in gdown.download_folder: {e}"
            print("[gdrive] " + reason)
            log_failed_google_download(url, out_dir, reason)
            return


def find_links_on_page(page_url: str):
    """
    Fetches a page and yields all absolute hrefs found.
    """
    session = get_session()
    print(f"[page] Fetching {page_url}")
    r = session.get(page_url, timeout=120)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        abs_url = urlparse.urljoin(r.url, href)
        yield abs_url


def looks_like_direct_file(url: str) -> bool:
    """
    Heuristic: treat URLs ending with common document extensions as direct files.
    """
    parsed = urlparse.urlparse(url)
    path = parsed.path.lower()
    exts = (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".zip", ".txt")
    return path.endswith(exts)


# ----------------- Main logic -----------------

def process_url(url: str) -> None:
    data_dir = ensure_data_dir()

    # If it's a direct Drive or Dropbox link, handle directly
    if is_google_drive_url(url):
        download_google_drive(url, data_dir)
        return

    if is_dropbox_url(url):
        download_dropbox_file(url, data_dir)
        return

    # Otherwise, treat as HTML page and scrape links
    seen = set()
    for link in find_links_on_page(url):
        if link in seen:
            continue
        seen.add(link)

        if is_google_drive_url(link):
            try:
                download_google_drive(link, data_dir)
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
        else:
            # Not a recognized asset type; ignore.
            continue


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python download_assets.py <url>")
        sys.exit(1)

    target_url = sys.argv[1]
    process_url(target_url)
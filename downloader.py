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

import requests
from bs4 import BeautifulSoup

try:
    import gdown
except ImportError:
    gdown = None


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


def download_google_drive(url: str, out_dir: Path) -> None:
    """
    Use gdown to download from Google Drive.

    - If it's a folder, gdown will recreate the folder structure under out_dir.
    - If it's a single file, it will save into out_dir.
    """
    if gdown is None:
        raise RuntimeError(
            "gdown is not installed. Install it with: pip install gdown"
        )

    parsed = urlparse.urlparse(url)
    path = parsed.path or ""

    print(f"[gdrive] Downloading from Google Drive: {url}")
    if "/folders/" in path or "/drive/folders/" in path:
        # Folder
        gdown.download_folder(
            url=url,
            output=str(out_dir),
            quiet=False,
            use_cookies=False
        )
    else:
        # File
        gdown.download(
            url=url,
            output=str(out_dir),
            quiet=False,
            fuzzy=True
        )


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
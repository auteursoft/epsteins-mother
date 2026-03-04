#!/usr/bin/env python3
"""
Download all PDFs linked from the FBI Vault Jeffrey Epstein page into ./fbi-pdfs.

Behavior:
- Creates ./fbi-pdfs if it doesn't exist.
- Downloads every PDF found.
- If a target filename already exists, saves another copy with _YYYYMMDD before .pdf
  (and if that also exists, appends _YYYYMMDD_1, _YYYYMMDD_2, ...).
"""

from __future__ import annotations

import datetime as dt
import os
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse, unquote

import requests
from bs4 import BeautifulSoup


START_URL = "https://vault.fbi.gov/jeffrey-epstein"
OUT_DIR = Path.cwd() / "fbi-pdfs"
TIMEOUT = 60


def ensure_out_dir() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def today_yyyymmdd() -> str:
    # Local date of the machine running the script
    return dt.date.today().strftime("%Y%m%d")


def safe_filename(name: str) -> str:
    # Keep it filesystem-friendly (Windows/macOS/Linux)
    name = name.strip().replace("\u00a0", " ")
    name = re.sub(r"[<>:\"/\\|?*\x00-\x1F]", "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name or "download.pdf"


def filename_from_headers_or_url(resp: requests.Response, url: str) -> str:
    cd = resp.headers.get("content-disposition", "")
    # Try RFC 6266-ish: filename="..."
    m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cd, flags=re.IGNORECASE)
    if m:
        return safe_filename(unquote(m.group(1)))

    # Fallback: URL path basename
    path = urlparse(url).path
    base = os.path.basename(path)
    base = unquote(base)
    if not base:
        base = "download.pdf"
    return safe_filename(base)


def uniquify_path(path: Path, date_tag: str) -> Path:
    """
    If path exists, return a new path with _YYYYMMDD before .pdf.
    If that also exists, add _YYYYMMDD_1, _YYYYMMDD_2, ...
    """
    if not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix or ".pdf"
    if suffix.lower() != ".pdf":
        # If something odd happens, still keep extension
        suffix = path.suffix

    candidate = path.with_name(f"{stem}_{date_tag}{suffix}")
    if not candidate.exists():
        return candidate

    i = 1
    while True:
        candidate_i = path.with_name(f"{stem}_{date_tag}_{i}{suffix}")
        if not candidate_i.exists():
            return candidate_i
        i += 1


def looks_like_pdf_url(u: str) -> bool:
    u_low = u.lower()
    return (
        u_low.endswith(".pdf")
        or "/at_download/file" in u_low
        or "at_download=file" in u_low
    )


def fetch_html(session: requests.Session, url: str) -> BeautifulSoup:
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def discover_pdf_urls(session: requests.Session, start_url: str) -> list[str]:
    """
    1) Grab all links from the start page
    2) Keep direct PDF links
    3) For non-direct links (notably /view pages), fetch them and search for PDF download links
    """
    soup = fetch_html(session, start_url)

    raw_links: set[str] = set()
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        abs_url = urljoin(start_url, href)
        # Stay within vault.fbi.gov
        if urlparse(abs_url).netloc and urlparse(abs_url).netloc != "vault.fbi.gov":
            continue
        raw_links.add(abs_url)

    pdf_urls: set[str] = set()
    maybe_pages: set[str] = set()

    for u in raw_links:
        if looks_like_pdf_url(u):
            pdf_urls.add(u)
        else:
            maybe_pages.add(u)

    # Crawl likely "view" pages to find /at_download/file links
    for page_url in sorted(maybe_pages):
        # Heuristic: only follow Epstein-related pages to avoid wandering the whole site
        if "jeffrey-epstein" not in page_url:
            continue
        try:
            psoup = fetch_html(session, page_url)
        except Exception:
            continue

        for a in psoup.select("a[href]"):
            href = a.get("href")
            if not href:
                continue
            abs_url = urljoin(page_url, href)
            if urlparse(abs_url).netloc and urlparse(abs_url).netloc != "vault.fbi.gov":
                continue
            if looks_like_pdf_url(abs_url):
                pdf_urls.add(abs_url)

    return sorted(pdf_urls)


def download_pdf(session: requests.Session, pdf_url: str, date_tag: str) -> Path:
    with session.get(pdf_url, stream=True, timeout=TIMEOUT) as r:
        r.raise_for_status()

        # Some links may not end in .pdf; trust headers/url and enforce .pdf if needed.
        fname = filename_from_headers_or_url(r, pdf_url)
        if not fname.lower().endswith(".pdf"):
            fname = f"{Path(fname).stem}.pdf"

        target = OUT_DIR / fname
        target = uniquify_path(target, date_tag)

        tmp = target.with_suffix(target.suffix + ".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
        tmp.replace(target)
        return target


def main() -> int:
    ensure_out_dir()
    date_tag = today_yyyymmdd()

    with requests.Session() as session:
        session.headers.update(
            {
                "User-Agent": "pdf-downloader/1.0 (+https://vault.fbi.gov/)",
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*;q=0.8",
            }
        )

        print(f"Discovering PDF links from: {START_URL}")
        pdf_urls = discover_pdf_urls(session, START_URL)
        if not pdf_urls:
            print("No PDF links found. The page structure may have changed.", file=sys.stderr)
            return 2

        print(f"Found {len(pdf_urls)} PDF URL(s). Downloading into: {OUT_DIR}")
        for i, url in enumerate(pdf_urls, start=1):
            try:
                out_path = download_pdf(session, url, date_tag)
                print(f"[{i}/{len(pdf_urls)}] OK  {out_path.name}")
            except Exception as e:
                print(f"[{i}/{len(pdf_urls)}] ERR {url} -> {e}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
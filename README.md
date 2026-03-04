# epsteins-mother
The "Epstein Files", with allusions to Juan Epstein. 

## DOJ Reveal
Files made available for download here, in a Google Drive: https://drive.google.com/drive/folders/1hTNH5woIRio578onLGElkTWofUSWRoH_ 

The drive is referenced here: https://oversight.house.gov/release/oversight-committee-releases-additional-epstein-estate-documents/ 

```bash
python downloader.py https://drive.google.com/drive/folders/1hTNH5woIRio578onLGElkTWofUSWRoH_ 
```

### Downloader Notes: 

Then make sure you're logged into Google Drive in Firefox (or Chrome) before running the script. The cookie extraction happens automatically at startup.

What changed and why:

The biggest structural change is that folder downloads no longer depend on gdown running to completion. The script tries gdown first (since it handles subfolder structure well), but if gdown hits a FileURLRetrievalError, it falls back to independently enumerating the folder contents by scraping the folder HTML page, then downloading each file one at a time with its own retry loop and exponential backoff. This means file #247 failing doesn't prevent files #248–500 from downloading.

The cookie pipeline works in two directions simultaneously: it writes Netscape-format cookies to ~/.cache/gdown/cookies.txt so gdown uses your browser session, and it also injects the same cookies into the requests.Session used by the direct-GET fallback path. This means both gdown and the fallback bypass Google's "too many accesses" throttle as long as you can access the file in your browser.

The confirm=t parameter in the direct GET URL is a trick that bypasses Google's virus-scan interstitial for large files without needing to parse the confirmation page (though the script does that too as a second fallback).

One thing to watch: browser_cookie3 needs Firefox to be closed on some platforms (especially macOS) to read the cookie database. If you get a "database is locked" error, close Firefox, run the script, then reopen it.

## FBI Reveal

https://vault.fbi.gov/jeffrey-epstein 

```bash
python fbi-downloader.py
```

## Getting Files
1. Do the usual work to create your python virtual environment: 
 - `python3 -m venv .venv`
 - `source .venv/bin/activate`
 - `pip install -r requirements.txt`
2. Run the downloader: `python downloader.py`




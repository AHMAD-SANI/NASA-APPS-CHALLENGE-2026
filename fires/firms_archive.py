"""Download and verify NASA FIRMS yearly per-country active-fire archive files.

The files stay on disk, described by a manifest, and are deliberately not
loaded into ``FireDetection``: a multi-decade archive would dwarf the live
dataset. Later aggregation reads the verified files directly.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import time
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

import requests

BASE_URL = 'https://firms.modaps.eosdis.nasa.gov/data/country'
MANIFEST_NAME = 'manifest.json'
CHUNK_SIZE = 1024 * 1024
MAX_ATTEMPTS = 4
MAX_BAD_ROW_FRACTION = 0.01

# First year each product family can have data. A year the archive has not
# published yet comes back as 404 and is recorded as unavailable.
PRODUCT_FIRST_YEAR = {
    'modis': 2000,
    'viirs-snpp': 2012,
    # FIRMS names these folders after the JPSS mission: JPSS-1 is NOAA-20 and
    # JPSS-2 is NOAA-21.
    'viirs-jpss1': 2018,
    'viirs-jpss2': 2023,
}

REQUIRED_COLUMNS = {
    'latitude', 'longitude', 'acq_date', 'acq_time', 'satellite', 'instrument',
    'confidence', 'version', 'frp', 'daynight', 'type',
}

COUNTRY_PATTERN = re.compile(r'^[A-Za-z][A-Za-z_\-]*$')


class ArchiveFileInvalid(Exception):
    """The downloaded file is not a usable FIRMS archive file."""


def file_url(product: str, year: int, country: str) -> str:
    return f'{BASE_URL}/{product}/{year}/{product}_{year}_{country}.csv'


def relative_path(product: str, year: int, country: str) -> str:
    return f'{country}/{product}/{product}_{year}_{country}.csv'


def manifest_key(product: str, year: int, country: str) -> str:
    return f'{country}/{product}/{year}'


def validate_country(country: str) -> str:
    # The name becomes both a URL segment and a directory name.
    if not COUNTRY_PATTERN.match(country):
        raise ValueError(f'Invalid FIRMS country name: {country!r}. Use the FIRMS file name, e.g. Nigeria or Burkina_Faso.')
    return country


def load_manifest(root: Path) -> dict:
    path = root / MANIFEST_NAME
    if not path.exists():
        return {'source': BASE_URL, 'files': {}}
    return json.loads(path.read_text(encoding='utf-8'))


def save_manifest(root: Path, manifest: dict) -> None:
    root.mkdir(parents=True, exist_ok=True)
    temporary = root / f'{MANIFEST_NAME}.part'
    temporary.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding='utf-8')
    os.replace(temporary, root / MANIFEST_NAME)


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(CHUNK_SIZE), b''):
            digest.update(chunk)
    return digest.hexdigest()


def inspect_csv(path: Path, year: int) -> dict:
    """Profile a downloaded file, raising ArchiveFileInvalid if it is unusable."""
    rows = bad_rows = out_of_year = 0
    days: set[date] = set()
    versions: Counter = Counter()
    satellites: Counter = Counter()
    types: Counter = Counter()

    with path.open('r', encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise ArchiveFileInvalid(f'Missing columns: {sorted(missing)}')
        for row in reader:
            rows += 1
            try:
                latitude, longitude = float(row['latitude']), float(row['longitude'])
                acquired = date.fromisoformat(row['acq_date'])
            except (TypeError, ValueError):
                bad_rows += 1
                continue
            if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
                bad_rows += 1
                continue
            if acquired.year != year:
                out_of_year += 1
                continue
            days.add(acquired)
            versions[row['version']] += 1
            satellites[row['satellite']] += 1
            types[row['type']] += 1

    if rows == 0:
        raise ArchiveFileInvalid('File has no data rows')
    if (bad_rows + out_of_year) / rows > MAX_BAD_ROW_FRACTION:
        raise ArchiveFileInvalid(f'{bad_rows} malformed and {out_of_year} out-of-year rows out of {rows}')

    return {
        'rows': rows,
        'bad_rows': bad_rows,
        'out_of_year_rows': out_of_year,
        'days_with_detections': len(days),
        'first_date': min(days).isoformat() if days else None,
        'last_date': max(days).isoformat() if days else None,
        'versions': dict(versions),
        'satellites': dict(satellites),
        'types': dict(types),
    }


def _download(session, url: str, destination: Path) -> str:
    """Stream ``url`` to ``destination``. Returns 'ok' or 'unavailable'."""
    response = session.get(url, stream=True, timeout=(30, 120))
    try:
        if response.status_code == 404:
            return 'unavailable'
        response.raise_for_status()
        expected = response.headers.get('Content-Length')
        written = 0
        with destination.open('wb') as handle:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                handle.write(chunk)
                written += len(chunk)
    finally:
        response.close()
    if expected is not None and int(expected) != written:
        raise requests.ConnectionError(f'Truncated download: got {written} of {expected} bytes')
    return 'ok'


def fetch_year(root: Path, manifest: dict, product: str, year: int, country: str, session, force: bool = False, sleep=time.sleep) -> dict:
    """Fetch one file and record the outcome in ``manifest``. Never raises for
    a per-file failure, so one bad year cannot abort a long backfill."""
    key = manifest_key(product, year, country)
    relative = relative_path(product, year, country)
    target = root / relative
    previous = manifest['files'].get(key)

    if not force and previous and previous.get('status') == 'verified' and target.exists():
        if sha256_of(target) == previous.get('sha256'):
            return {**previous, 'skipped': True}

    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f'{target.name}.part')
    url = file_url(product, year, country)
    entry: dict = {
        'product': product, 'year': year, 'country': country, 'url': url,
        'retrieved_at': datetime.now(timezone.utc).isoformat(),
    }

    last_error = ''
    try:
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                outcome = _download(session, url, temporary)
                if outcome == 'unavailable':
                    entry['status'] = 'unavailable'
                    break
                entry.update(inspect_csv(temporary, year))
                entry.update(status='verified', path=relative, bytes=temporary.stat().st_size, sha256=sha256_of(temporary))
                os.replace(temporary, target)
                break
            except ArchiveFileInvalid as exc:
                entry.update(status='invalid', error=str(exc))
                break
            except (requests.RequestException, UnicodeDecodeError, OSError) as exc:
                last_error = str(exc)
                if attempt < MAX_ATTEMPTS:
                    sleep(2 ** attempt)
        else:
            entry.update(status='failed', error=last_error)
    finally:
        temporary.unlink(missing_ok=True)

    # A failed re-fetch must not erase the record of a good file on disk.
    if not (entry['status'] != 'verified' and previous and previous.get('status') == 'verified' and target.exists()):
        manifest['files'][key] = entry
    return entry

"""Prepare the multi-year harmonized archive for any country on demand.

The harmonized index reads ``ArchiveCellDay`` rows built from FIRMS yearly
per-country files. This module downloads and aggregates those files for one
country in a background thread, so the frontend can ask for a country that has
not been archived yet and follow the progress.

Raw CSVs are deleted once aggregated (the index only needs the aggregated
grid), which keeps large countries from filling the disk.
"""
from __future__ import annotations

import re
import shutil
import threading
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from django.conf import settings
from django.db import close_old_connections
from django.utils import timezone as dj_timezone

from . import archive_aggregation as aggregation
from . import firms_archive as archive
from .live_scheduler import MIN_FREE_BYTES
from .models import ArchiveFile

DEFAULT_ROOT = 'analysis/data/raw/firms_country'
PROBE_PRODUCT, PROBE_YEAR = 'modis', 2020

# Names whose FIRMS spelling cannot be derived from the ISO name.
_OVERRIDES = {
    'Viet Nam': ['Vietnam'],
    "Lao People's Democratic Republic": ['Laos', 'Lao_PDR', 'Lao_Peoples_Democratic_Republic'],
    'Congo': ['Republic_of_Congo', 'Republic_of_the_Congo', 'Congo_Brazzaville', 'Congo'],
    "Korea, Democratic People's Republic of": ['North_Korea', 'Democratic_Peoples_Republic_of_Korea'],
    'Syrian Arab Republic': ['Syria'],
    'Moldova, Republic of': ['Moldova', 'Republic_of_Moldova'],
    'Micronesia, Federated States of': ['Micronesia', 'Federated_States_of_Micronesia'],
}

_STATE: dict[str, dict] = {}
_LOCK = threading.Lock()
_RUN_LOCK = threading.Lock()  # one country at a time: aggregation is SQLite heavy


def _slug(text: str) -> str:
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode()
    text = re.sub(r"[^A-Za-z\-]+", '_', text).strip('_')
    return re.sub(r'_+', '_', text)


def name_candidates(name: str) -> list[str]:
    """Likely FIRMS file-name spellings for an ISO country name, best first."""
    out = list(_OVERRIDES.get(name, []))
    out.append(_slug(name))
    if ',' in name:
        head, tail = (part.strip() for part in name.split(',', 1))
        out.append(_slug(head))
        reversed_name = re.sub(r'^(the|The)\s+', '', f'{tail} {head}')
        out.append(_slug(reversed_name))
    seen, result = set(), []
    for candidate in out:
        if candidate and candidate not in seen and archive.COUNTRY_PATTERN.match(candidate):
            seen.add(candidate)
            result.append(candidate)
    return result


def resolve_firms_name(name: str, session) -> str | None:
    # Already archived under one of the candidate names: no network needed.
    candidates = name_candidates(name)
    archived = set(ArchiveFile.objects.filter(country__in=candidates).values_list('country', flat=True))
    for candidate in candidates:
        if candidate in archived:
            return candidate
    for candidate in candidates:
        try:
            response = session.head(archive.file_url(PROBE_PRODUCT, PROBE_YEAR, candidate), timeout=30)
        except requests.RequestException:
            continue
        if response.status_code == 200:
            return candidate
    return None


def _root() -> Path:
    root = Path(DEFAULT_ROOT)
    return root if root.is_absolute() else Path(settings.BASE_DIR) / root


def _years(product: str) -> range:
    return range(archive.PRODUCT_FIRST_YEAR[product], datetime.now(timezone.utc).year + 1)


def _is_current(country: str, product: str, year: int) -> bool:
    existing = ArchiveFile.objects.filter(country=country, product=product, year=year).first()
    if existing is None or existing.aggregation_version != aggregation.AGGREGATION_VERSION:
        return False
    # The current year's file keeps growing; refresh it once a week.
    return year < datetime.now(timezone.utc).year or existing.aggregated_at > dj_timezone.now() - timedelta(days=7)


def status(country_code: str) -> dict:
    with _LOCK:
        return dict(_STATE.get(country_code.upper(), {'status': 'idle'}))


def _update(code: str, **values) -> None:
    with _LOCK:
        _STATE.setdefault(code, {}).update(values, updated_at=datetime.now(timezone.utc).isoformat())


def start(country_code: str, country_name: str) -> dict:
    code = country_code.upper()
    with _LOCK:
        current = _STATE.get(code)
        if current and current.get('status') in {'queued', 'resolving', 'downloading', 'aggregating'}:
            return dict(current)
    if shutil.disk_usage(_root().parent if _root().exists() else settings.BASE_DIR).free < MIN_FREE_BYTES:
        _update(code, status='failed', progress=0, country=country_name,
                message='Not enough free disk space to download the archive (needs at least 3 GB free).')
        return status(code)
    _update(code, status='queued', progress=0, country=country_name, message='Waiting for another archive job to finish.')
    threading.Thread(target=_run, args=(code, country_name), daemon=True, name=f'archive-prepare-{code}').start()
    return status(code)


def _run(code: str, name: str) -> None:
    with _RUN_LOCK:
        close_old_connections()
        try:
            _prepare(code, name)
        except Exception as exc:  # report, never crash the server thread
            _update(code, status='failed', message=f'Archive preparation failed: {exc}')
        finally:
            close_old_connections()


def _prepare(code: str, name: str) -> None:
    session = requests.Session()
    session.headers['User-Agent'] = 'FireScope-archive-fetch'
    _update(code, status='resolving', progress=1, message='Finding the FIRMS archive files for this country.')
    firms_name = resolve_firms_name(name, session)
    if firms_name is None:
        _update(code, status='failed', progress=0,
                message=f'NASA FIRMS has no yearly country archive under any known spelling of "{name}".')
        return

    root = _root()
    tasks = [(product, year) for product in archive.PRODUCT_FIRST_YEAR for year in _years(product)]
    todo = [(p, y) for p, y in tasks if not _is_current(firms_name, p, y)]
    manifest = archive.load_manifest(root)
    failures = []
    for index, (product, year) in enumerate(todo, start=1):
        if shutil.disk_usage(root.parent if root.exists() else settings.BASE_DIR).free < MIN_FREE_BYTES:
            failures.append('stopped early: less than 3 GB of disk space left')
            break
        _update(code, status='downloading', firms_name=firms_name,
                progress=2 + int(96 * (index - 1) / max(1, len(todo))),
                message=f'{product} {year}: downloading ({index} of {len(todo)} files)')
        entry = archive.fetch_year(root, manifest, product, year, firms_name, session, force=True)
        archive.save_manifest(root, manifest)
        if entry['status'] == 'unavailable':
            continue
        if entry['status'] != 'verified':
            failures.append(f"{product} {year}: {entry['status']} {entry.get('error', '')}".strip())
            continue
        _update(code, status='aggregating', message=f'{product} {year}: aggregating ({index} of {len(todo)} files)')
        try:
            aggregation.aggregate_manifest_entry(root, entry, force=True)
        except (aggregation.AggregationError, OSError) as exc:
            failures.append(f'{product} {year}: aggregation failed: {exc}')
            continue
        # The grid now holds everything the index needs; free the disk.
        (root / entry['path']).unlink(missing_ok=True)
        manifest['files'][archive.manifest_key(product, year, firms_name)]['status'] = 'aggregated_raw_removed'
        archive.save_manifest(root, manifest)

    years = ArchiveFile.objects.filter(country=firms_name).count()
    if not years:
        _update(code, status='failed', progress=100, message='; '.join(failures) or 'No archive files could be prepared.')
    else:
        _update(code, status='partial' if failures else 'ready', progress=100, archived_files=years,
                message='; '.join(failures)[:1000] if failures else f'{years} yearly files archived for {name}.')

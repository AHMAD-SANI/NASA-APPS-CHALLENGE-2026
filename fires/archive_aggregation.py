"""Aggregate verified FIRMS archive files into grid-cell, day and platform counts."""
from __future__ import annotations

import csv
import math
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

from django.db import transaction

from fires import firms_archive as archive
from fires.models import ArchiveCellDay, ArchiveFile

AGGREGATION_VERSION = 1
GRID_DEGREES = 0.25
VEGETATION_TYPE = '0'
MODIS_LOW_CONFIDENCE_BELOW = 30  # FIRMS low-confidence class for MODIS.
INSERT_BATCH = 5000
# A platform-year counts as complete only if detections reach within this many
# days of both year ends. Partial years must not enter seasonal baselines.
COMPLETE_YEAR_TOLERANCE_DAYS = 31

# Each satellite stays separate: Aqua joins Terra in 2002, so pooling them
# would create an artificial jump in the long-term record.
PLATFORMS = {
    'Terra': 'MODIS_TERRA',
    'Aqua': 'MODIS_AQUA',
    'N': 'VIIRS_SNPP',
    'N20': 'VIIRS_NOAA20',
}


class AggregationError(Exception):
    """A file could not be aggregated without losing track of its rows."""


@dataclass
class CellTotals:
    detections: int = 0
    daytime: int = 0
    nighttime: int = 0
    low_confidence: int = 0
    frp_sum: float = 0.0


@dataclass
class FileAggregate:
    rows_read: int = 0
    vegetation_rows: int = 0
    rejected_rows: int = 0
    non_vegetation: Counter = field(default_factory=Counter)
    cells: dict = field(default_factory=dict)


def grid_index(coordinate: float) -> int:
    return math.floor(coordinate / GRID_DEGREES)


def is_low_confidence(platform: str, confidence: str) -> bool:
    if platform.startswith('MODIS'):
        try:
            return float(confidence) < MODIS_LOW_CONFIDENCE_BELOW
        except ValueError:
            return False
    return confidence.strip().lower() == 'l'


def _frp(value: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return parsed if math.isfinite(parsed) else 0.0


def aggregate_csv(path: Path, year: int) -> FileAggregate:
    result = FileAggregate()
    with path.open('r', encoding='utf-8', newline='') as handle:
        for row in csv.DictReader(handle):
            result.rows_read += 1
            try:
                latitude, longitude = float(row['latitude']), float(row['longitude'])
                acquired = date.fromisoformat(row['acq_date'])
            except (TypeError, ValueError):
                result.rejected_rows += 1
                continue
            if acquired.year != year or not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
                result.rejected_rows += 1
                continue
            if row['type'] != VEGETATION_TYPE:
                result.non_vegetation[row['type']] += 1
                continue
            platform = PLATFORMS.get(row['satellite'])
            if platform is None:
                raise AggregationError(f"Unknown satellite label {row['satellite']!r} in {path.name}")

            key = (platform, acquired, grid_index(latitude), grid_index(longitude))
            totals = result.cells.get(key)
            if totals is None:
                totals = result.cells[key] = CellTotals()
            totals.detections += 1
            totals.daytime += row['daynight'] == 'D'
            totals.nighttime += row['daynight'] == 'N'
            totals.low_confidence += is_low_confidence(platform, row['confidence'])
            totals.frp_sum += _frp(row['frp'])
            result.vegetation_rows += 1

    accounted = result.vegetation_rows + result.rejected_rows + sum(result.non_vegetation.values())
    if accounted != result.rows_read:
        raise AggregationError(f'{path.name}: accounted for {accounted} of {result.rows_read} rows')
    return result


def platform_stats(result: FileAggregate, year: int) -> dict:
    days: dict[str, set] = {}
    detections: Counter = Counter()
    for (platform, acquired, _, _), totals in result.cells.items():
        days.setdefault(platform, set()).add(acquired)
        detections[platform] += totals.detections

    tolerance = timedelta(days=COMPLETE_YEAR_TOLERANCE_DAYS)
    stats = {}
    for platform, seen in days.items():
        first, last = min(seen), max(seen)
        stats[platform] = {
            'first_date': first.isoformat(),
            'last_date': last.isoformat(),
            'detections': detections[platform],
            'days_with_detections': len(seen),
            'complete_year': first - date(year, 1, 1) <= tolerance and date(year, 12, 31) - last <= tolerance,
        }
    return stats


def aggregate_manifest_entry(root: Path, entry: dict, force: bool = False) -> tuple[str, ArchiveFile | None]:
    """Aggregate one verified file, replacing any earlier result atomically."""
    identity = {'country': entry['country'], 'product': entry['product'], 'year': entry['year']}
    existing = ArchiveFile.objects.filter(**identity).first()
    if not force and existing and existing.sha256 == entry['sha256'] and existing.aggregation_version == AGGREGATION_VERSION:
        return 'skipped', existing

    path = root / entry['path']
    if archive.sha256_of(path) != entry['sha256']:
        raise AggregationError(f'{path.name}: checksum does not match the manifest')

    result = aggregate_csv(path, entry['year'])
    if result.rows_read != entry['rows']:
        raise AggregationError(f"{path.name}: read {result.rows_read} rows, manifest has {entry['rows']}")

    with transaction.atomic():
        if existing:
            # _raw_delete avoids loading every child row just to cascade.
            ArchiveCellDay.objects.filter(source_file=existing)._raw_delete(using='default')
            existing.delete()
        source = ArchiveFile.objects.create(
            **identity,
            sha256=entry['sha256'],
            aggregation_version=AGGREGATION_VERSION,
            grid_degrees=GRID_DEGREES,
            rows_read=result.rows_read,
            vegetation_rows=result.vegetation_rows,
            non_vegetation_rows=dict(result.non_vegetation),
            rejected_rows=result.rejected_rows,
            platform_stats=platform_stats(result, entry['year']),
        )
        ArchiveCellDay.objects.bulk_create(
            (
                ArchiveCellDay(
                    source_file=source, platform=platform, acq_date=acquired, cell_lat=cell_lat, cell_lon=cell_lon,
                    detections=totals.detections, daytime=totals.daytime, nighttime=totals.nighttime,
                    low_confidence=totals.low_confidence, frp_sum=totals.frp_sum,
                )
                for (platform, acquired, cell_lat, cell_lon), totals in result.cells.items()
            ),
            batch_size=INSERT_BATCH,
        )
    return 'aggregated', source

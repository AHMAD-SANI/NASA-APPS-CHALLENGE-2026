"""Harmonized relative burning-activity index built from the FIRMS archive.

Detection counts from different sensors are not interchangeable, so they are
never converted or added together. Each platform's activity is instead ranked
against that same platform's own history for the same time of year. The
resulting percentiles are comparable across sensors. The index describes how
unusual activity is; it is not an estimate of the number of fires.
"""
from __future__ import annotations

import calendar
import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
from django.core.cache import caches
from django.db import connection
from django.db.models import Max

from fires.archive_aggregation import GRID_DEGREES, grid_index
from fires.models import ArchiveCellDay, ArchiveFile

DAY_SLOTS = 366
DAILY_WINDOW_DAYS = 7  # Days either side of the target date in the reference.
MIN_DAILY_REFERENCE_VALUES = 30
MIN_MONTHLY_REFERENCE_VALUES = 5
MAX_COVERAGE_SAMPLE = 2500
PARTIAL_COVERAGE_BELOW = 0.9
UNAVAILABLE_COVERAGE_BELOW = 0.5
CACHE_ALIAS = 'harmonized'
CACHE_SECONDS = 30 * 24 * 3600  # Keys include the archive version, so entries cannot go stale.

FAMILIES = {
    'MODIS': ('MODIS_TERRA', 'MODIS_AQUA'),
    'VIIRS': ('VIIRS_SNPP', 'VIIRS_NOAA20'),
}
PLATFORMS = tuple(platform for members in FAMILIES.values() for platform in members)

# VIIRS S-NPP's archive starts on 20 January 2012, inside the peak dry season
# for much of West Africa, so that year is not a fair reference.
EXCLUDED_REFERENCE_YEARS = {'VIIRS_SNPP': {2012}}
# Terra's counts fall away from every other platform after 2022, while its
# orbit drifts late in the mission. Later Terra data is not used at all.
PLATFORM_LAST_USABLE = {'MODIS_TERRA': date(2022, 12, 31)}

# Upper percentile bound for each class, ascending.
CATEGORIES = (
    (20, 'below_normal'),
    (80, 'normal'),
    (95, 'above_normal'),
    (100.01, 'extreme'),
)


def percentile_rank(value: float, reference: np.ndarray) -> float:
    """Mid-rank percentile, so ties (common at zero) score at their centre."""
    return float(np.mean(reference < value) * 100 + np.mean(reference == value) * 50)


def categorize(index: float) -> str:
    return next(label for bound, label in CATEGORIES if index < bound)


def day_slot(day: date) -> int:
    # Slots follow a leap year, so a date keeps its slot in every year.
    return date(2000, day.month, day.day).timetuple().tm_yday - 1


@dataclass
class PlatformSeries:
    """Daily detections by year and day slot. NaN means no usable observation."""

    by_year: dict
    reference_years: set


def _archive_token() -> str:
    stats = ArchiveFile.objects.aggregate(latest=Max('aggregated_at'), last_id=Max('id'))
    return f"{stats['last_id']}:{stats['latest']}"


def _archive_cells(token: str) -> frozenset:
    key = f'harmonized-cells-{hashlib.sha256(token.encode()).hexdigest()}'
    cells = caches[CACHE_ALIAS].get(key)
    if cells is None:
        cells = frozenset(ArchiveCellDay.objects.order_by().values_list('cell_lat', 'cell_lon').distinct())
        caches[CACHE_ALIAS].set(key, cells, CACHE_SECONDS)
    return cells


def _cell_center(index: int) -> float:
    return (index + 0.5) * GRID_DEGREES


def _prepare_polygons(geometry: dict) -> list:
    """Convert GeoJSON to arrays, one list of rings (outer first) per polygon."""
    polygons = geometry['coordinates'] if geometry['type'] == 'MultiPolygon' else [geometry['coordinates']]
    return [[np.asarray(ring, dtype=float)[:, :2] for ring in polygon] for polygon in polygons]


def _ring_contains(longitude: float, latitude: float, ring: np.ndarray) -> bool:
    # Ray casting over numpy arrays: a detailed national border has so many
    # vertices that a Python loop takes over a minute per area.
    x, y = ring[:, 0], ring[:, 1]
    next_x, next_y = np.roll(x, -1), np.roll(y, -1)
    crosses = (y > latitude) != (next_y > latitude)
    x, y, next_x, next_y = x[crosses], y[crosses], next_x[crosses], next_y[crosses]
    intersections = x + (next_x - x) * (latitude - y) / (next_y - y)
    return bool(np.count_nonzero(longitude < intersections) % 2)


def polygons_contain(longitude: float, latitude: float, polygons: list) -> bool:
    return any(
        _ring_contains(longitude, latitude, rings[0])
        and not any(_ring_contains(longitude, latitude, hole) for hole in rings[1:])
        for rings in polygons
    )


def _in_area(latitude: float, longitude: float, bounds: dict, polygons: list | None) -> bool:
    if not (bounds['south'] <= latitude <= bounds['north'] and bounds['west'] <= longitude <= bounds['east']):
        return False
    return polygons is None or polygons_contain(longitude, latitude, polygons)


def resolve_area(bounds: dict | None, polygon) -> dict:
    """Find the archived cells in the area and how much of it the archive covers."""
    if bounds is None:
        return {
            'status': 'unavailable', 'cells': frozenset(), 'coverage_fraction': 0.0,
            'message': 'The historical archive does not cover the whole world. Select a region or bounding box inside an archived country.',
        }
    token = _archive_token()
    # Testing cells against a detailed national border is slow, so remember it.
    fingerprint = json.dumps([token, bounds, polygon], sort_keys=True)
    key = f'harmonized-area-{hashlib.sha256(fingerprint.encode()).hexdigest()}'
    area = caches[CACHE_ALIAS].get(key)
    if area is None:
        area = _compute_area(_archive_cells(token), bounds, polygon)
        caches[CACHE_ALIAS].set(key, area, CACHE_SECONDS)
    return area


def _compute_area(archived: frozenset, bounds: dict, polygon) -> dict:
    polygons = _prepare_polygons(polygon) if polygon else None
    memo: dict[tuple[int, int], bool] = {}

    def inside_area(cell: tuple[int, int]) -> bool:
        # Cells recur between the two passes below; border tests are slow.
        if cell not in memo:
            memo[cell] = _in_area(_cell_center(cell[0]), _cell_center(cell[1]), bounds, polygons)
        return memo[cell]

    cells = frozenset(cell for cell in archived if inside_area(cell))

    # Sample the area on the same grid, thinning it so huge areas stay cheap.
    lat_cells = range(grid_index(bounds['south']), grid_index(bounds['north']) + 1)
    lon_cells = range(grid_index(bounds['west']), grid_index(bounds['east']) + 1)
    step = max(1, int((len(lat_cells) * len(lon_cells) / MAX_COVERAGE_SAMPLE) ** 0.5))
    inside = covered = 0
    for lat in lat_cells[::step]:
        for lon in lon_cells[::step]:
            if inside_area((lat, lon)):
                inside += 1
                covered += (lat, lon) in archived
    fraction = covered / inside if inside else 0.0

    if not cells or fraction < UNAVAILABLE_COVERAGE_BELOW:
        status, message = 'unavailable', 'This area is not sufficiently covered by the downloaded historical archive.'
    elif fraction < PARTIAL_COVERAGE_BELOW:
        status, message = 'partial', 'Only part of this area is covered by the downloaded historical archive.'
    else:
        status, message = 'complete', ''
    return {'status': status, 'cells': cells, 'coverage_fraction': round(fraction, 3), 'message': message}


def _platform_spans() -> tuple[dict, dict]:
    """Return {platform: {year: (first, last)}} and each platform's reference years."""
    spans: dict = defaultdict(dict)
    complete: dict = defaultdict(lambda: defaultdict(lambda: True))
    for source in ArchiveFile.objects.all():
        for platform, stats in source.platform_stats.items():
            first, last = date.fromisoformat(stats['first_date']), date.fromisoformat(stats['last_date'])
            current = spans[platform].get(source.year)
            if current:
                first, last = min(first, current[0]), max(last, current[1])
            spans[platform][source.year] = (first, last)
            complete[platform][source.year] &= stats['complete_year']

    reference = {}
    for platform, years in spans.items():
        last_usable = PLATFORM_LAST_USABLE.get(platform, date.max)
        reference[platform] = {
            year for year in years
            if complete[platform][year]
            and year not in EXCLUDED_REFERENCE_YEARS.get(platform, set())
            and year <= last_usable.year
        }
    return spans, reference


def load_series(cells: frozenset) -> dict[str, PlatformSeries]:
    fingerprint = f'{_archive_token()}|{sorted(cells)}'
    key = f'harmonized-series-{hashlib.sha256(fingerprint.encode()).hexdigest()}'
    series = caches[CACHE_ALIAS].get(key)
    if series is not None:
        return series

    spans, reference = _platform_spans()
    series = {}
    for platform, years in spans.items():
        last_usable = PLATFORM_LAST_USABLE.get(platform, date.max)
        by_year = {}
        for year, (first, last) in years.items():
            values = np.full(DAY_SLOTS, np.nan)
            day = first
            while day <= min(last, last_usable):
                # Inside the operating span, no record means nothing detected.
                values[day_slot(day)] = 0.0
                day += timedelta(days=1)
            by_year[year] = values
        series[platform] = PlatformSeries(by_year=by_year, reference_years=reference[platform])

    # Raw rows skip per-row date conversion, which dominates load time at
    # millions of rows; each distinct date string is parsed only once.
    positions: dict[str, tuple[int, int]] = {}
    table = ArchiveCellDay._meta.db_table
    with connection.cursor() as cursor:
        cursor.execute(
            f'SELECT platform, acq_date, cell_lat, cell_lon, detections FROM {table} '
            'WHERE cell_lat BETWEEN %s AND %s AND cell_lon BETWEEN %s AND %s',
            [min(lat for lat, _ in cells), max(lat for lat, _ in cells),
             min(lon for _, lon in cells), max(lon for _, lon in cells)],
        )
        for platform, day, lat, lon, detections in cursor:
            if (lat, lon) not in cells:
                continue
            position = positions.get(day)
            if position is None:
                parsed = date.fromisoformat(str(day))
                position = positions[day] = (parsed.year, day_slot(parsed))
            values = series[platform].by_year[position[0]]
            if not np.isnan(values[position[1]]):
                values[position[1]] += detections

    caches[CACHE_ALIAS].set(key, series, CACHE_SECONDS)
    return series


def _combine(period: str, scores: dict[str, tuple[float, float, int]]) -> dict:
    """Build one output row from {platform: (detections, percentile, reference size)}."""
    family_scores = {}
    for family, members in FAMILIES.items():
        values = [scores[platform][1] for platform in members if platform in scores]
        if values:
            family_scores[family] = float(np.mean(values))

    # Averaging families first stops two VIIRS satellites outvoting MODIS.
    index = float(np.mean(list(family_scores.values())))
    total_detections = sum(entry[0] for entry in scores.values())
    return {
        'period': period,
        'index': round(index, 1),
        'category': 'no_detections' if total_detections == 0 else categorize(index),
        'modis_index': round(family_scores['MODIS'], 1) if 'MODIS' in family_scores else None,
        'viirs_index': round(family_scores['VIIRS'], 1) if 'VIIRS' in family_scores else None,
        'platforms': {
            platform: {'detections': int(entry[0]), 'percentile': round(entry[1], 1), 'reference_values': entry[2]}
            for platform, entry in sorted(scores.items())
        },
    }


def _reference_matrix(platform_series: PlatformSeries, excluded_year: int) -> np.ndarray | None:
    # The year being scored never sits in its own reference.
    years = [year for year in platform_series.reference_years if year != excluded_year]
    if not years:
        return None
    return np.stack([platform_series.by_year[year] for year in years])


def daily_index(series: dict[str, PlatformSeries], start: date, end: date) -> list[dict]:
    rows = []
    day = start
    while day <= end:
        slot = day_slot(day)
        window = [(slot + offset) % DAY_SLOTS for offset in range(-DAILY_WINDOW_DAYS, DAILY_WINDOW_DAYS + 1)]
        scores = {}
        for platform, platform_series in series.items():
            values = platform_series.by_year.get(day.year)
            if values is None or np.isnan(values[slot]):
                continue
            matrix = _reference_matrix(platform_series, day.year)
            if matrix is None:
                continue
            reference = matrix[:, window].ravel()
            reference = reference[~np.isnan(reference)]
            if reference.size >= MIN_DAILY_REFERENCE_VALUES:
                scores[platform] = (values[slot], percentile_rank(values[slot], reference), int(reference.size))
        if scores:
            rows.append(_combine(day.isoformat(), scores))
        day += timedelta(days=1)
    return rows


def _month_total(values: np.ndarray, year: int, month: int) -> float:
    """Total for a month, or NaN unless every day of it was observed."""
    first = day_slot(date(year, month, 1))
    return float(values[first:first + calendar.monthrange(year, month)[1]].sum())  # NaN propagates.


def monthly_index(series: dict[str, PlatformSeries], start_year: int | None = None, end_year: int | None = None) -> list[dict]:
    years = sorted({year for platform_series in series.values() for year in platform_series.by_year})
    rows = []
    for year in years:
        if (start_year and year < start_year) or (end_year and year > end_year):
            continue
        for month in range(1, 13):
            scores = {}
            for platform, platform_series in series.items():
                values = platform_series.by_year.get(year)
                if values is None:
                    continue
                total = _month_total(values, year, month)
                if np.isnan(total):
                    continue
                reference = np.array([
                    _month_total(platform_series.by_year[other], other, month)
                    for other in platform_series.reference_years if other != year
                ])
                reference = reference[~np.isnan(reference)]
                if reference.size >= MIN_MONTHLY_REFERENCE_VALUES:
                    scores[platform] = (total, percentile_rank(total, reference), int(reference.size))
            if scores:
                rows.append(_combine(f'{year}-{month:02d}', scores))
    return rows


def metadata(series: dict[str, PlatformSeries]) -> dict:
    return {
        'measure': 'Relative burning activity index (0-100). Not a fire count.',
        'method': (
            'Each platform is ranked against its own history for the same calendar period, excluding the '
            'year being scored. Platforms are averaged within MODIS and VIIRS, then the two families are averaged.'
        ),
        'reference_years': {platform: sorted(item.reference_years) for platform, item in sorted(series.items())},
        'caveats': [
            'No detection means none was recorded; cloud, smoke and satellite outages are not known.',
            'Before 2012 the index rests on MODIS alone.',
            'Long-term decline in MODIS counts may partly reflect sensor ageing, which can make recent periods rank lower.',
            f'Terra data after {PLATFORM_LAST_USABLE["MODIS_TERRA"].isoformat()} is not used.',
            f'Areas are approximated by {GRID_DEGREES} degree grid cells.',
        ],
    }

from __future__ import annotations

import math
import os
from datetime import date, timedelta

from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from analysis.fetch_data import date_chunks, fetch_availability_ranges, fetch_dataset_chunk
from fires.batching import safe_in_batch_size, safe_sql_batch_size
from fires.geography import point_in_geometry, resolve_country, resolve_region
from fires.management.commands.import_fire_data import _row_to_fire_detection
from fires.models import FireDetection
from fires.services import _FIRMS_DATASETS, _normalise_firms_row


DEFAULT_SENSORS = ('MODIS', 'VIIRS_SNPP', 'VIIRS_NOAA20', 'VIIRS_NOAA21')


def _available_days(ranges: list[tuple[date, date]], requested: list[date]) -> set[date]:
    return {
        day
        for day in requested
        if any(start <= day <= end for start, end in ranges)
    }


def _date_segments(days: set[date]) -> list[tuple[date, date]]:
    ordered = sorted(days)
    if not ordered:
        return []

    segments = []
    start = end = ordered[0]
    for day in ordered[1:]:
        if day == end + timedelta(days=1):
            end = day
        else:
            segments.append((start, end))
            start = end = day
    segments.append((start, end))
    return segments


def _requested_dates(start: date, end: date) -> list[date]:
    days = (end - start).days + 1
    return [start + timedelta(days=offset) for offset in range(days)]


def _product_plan(selections: list[str]) -> tuple[list[tuple[str, str]], dict[str, str]]:
    datasets: list[tuple[str, str]] = []
    fallbacks: dict[str, str] = {}
    seen: set[str] = set()

    for selection in selections:
        dataset, sensor = _FIRMS_DATASETS[selection]
        if dataset not in seen:
            datasets.append((dataset, sensor))
            seen.add(dataset)

        if selection == sensor and dataset.endswith('_SP'):
            fallback_dataset = f'{dataset[:-3]}_NRT'
            if fallback_dataset not in seen:
                datasets.append((fallback_dataset, sensor))
                seen.add(fallback_dataset)
            fallbacks[dataset] = fallback_dataset

    return datasets, fallbacks


def _region_filter(country: str | None, region: str | None):
    if not country:
        if region:
            raise CommandError('--country is required when --region is provided.')
        return None, None, 'global'

    resolved_country = resolve_country(country)
    if not resolved_country:
        raise CommandError(f'Unknown or unsupported country: {country}.')

    if not region:
        try:
            resolved, geometry = resolve_region({
                'country': resolved_country['name'],
                'type': 'ADM0',
                'name': resolved_country['name'],
            })
        except (RuntimeError, ValueError) as exc:
            raise CommandError(f'Could not resolve country boundary: {exc}') from exc
        return resolved['bbox'], geometry, resolved_country['name']

    matches = []
    errors = []
    for level in ('ADM1', 'ADM2'):
        try:
            resolved, geometry = resolve_region({
                'country': resolved_country['name'],
                'type': level,
                'name': region,
            })
            matches.append((resolved, geometry))
        except ValueError as exc:
            if str(exc).startswith('Unknown '):
                errors.append(str(exc))
                continue
            raise CommandError(f'Could not resolve region: {exc}') from exc
        except RuntimeError as exc:
            raise CommandError(f'Could not resolve region boundary: {exc}') from exc

    if not matches:
        detail = errors[-1] if errors else f'No matching administrative region named "{region}".'
        raise CommandError(detail)
    if len(matches) > 1:
        raise CommandError(
            f'Region name "{region}" matches more than one administrative level. '
            'Choose a unique region name.'
        )

    resolved, geometry = matches[0]
    return resolved['bbox'], geometry, f'{resolved_country["name"]} / {resolved["name"]}'


def _database_keys(keys: list[str]) -> set[str]:
    existing: set[str] = set()
    batch_size = safe_in_batch_size()
    for offset in range(0, len(keys), batch_size):
        existing.update(
            FireDetection.objects.filter(
                record_key__in=keys[offset:offset + batch_size]
            ).values_list('record_key', flat=True)
        )
    return existing


class Command(BaseCommand):
    help = 'Fetch NASA FIRMS detections and import them directly into the configured database.'

    def add_arguments(self, parser):
        parser.add_argument('--start-date', help='Inclusive start date in YYYY-MM-DD format.')
        parser.add_argument('--end-date', help='Inclusive end date in YYYY-MM-DD format.')
        parser.add_argument(
            '--sensors',
            nargs='+',
            choices=tuple(_FIRMS_DATASETS),
            default=None,
            help='Sensor shorthand or FIRMS product IDs. Defaults to MODIS and all VIIRS sensors.',
        )
        parser.add_argument('--country', help='Country name or ISO code for geographic filtering.')
        parser.add_argument(
            '--region',
            help='Administrative region name within --country (ADM1 or ADM2).',
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Maximum FireDetection records per database batch (default: 1000).',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Fetch and validate records without writing them to the database.',
        )

    def handle(self, *args, **options):
        today = timezone.now().date()
        try:
            end = date.fromisoformat(options['end_date']) if options['end_date'] else today
            start = (
                date.fromisoformat(options['start_date'])
                if options['start_date']
                else end - timedelta(days=4)
            )
        except ValueError as exc:
            raise CommandError('Dates must be valid ISO dates in YYYY-MM-DD format.') from exc
        if end < start:
            raise CommandError('--end-date must be on or after --start-date.')
        if options['batch_size'] < 1:
            raise CommandError('--batch-size must be greater than zero.')

        selections = options['sensors'] or list(DEFAULT_SENSORS)
        products, fallbacks = _product_plan(selections)
        bbox, geometry, scope = _region_filter(options['country'], options['region'])

        map_key = os.environ.get('FIRMS_MAP_KEY', '').strip()
        if not map_key:
            raise CommandError('FIRMS_MAP_KEY is not configured in the environment.')

        requested = _requested_dates(start, end)
        self.stdout.write(f'Requested date range: {start.isoformat()} to {end.isoformat()}')
        self.stdout.write(f'Products being fetched: {", ".join(dataset for dataset, _ in products)}')
        self.stdout.write(f'Geographic scope: {scope}')
        self.stdout.write(f'Mode: {"dry run (no database writes)" if options["dry_run"] else "database import"}')

        rows_downloaded = 0
        rows_inserted = 0
        rows_would_insert = 0
        duplicates_skipped = 0
        failed_records = 0
        failed_requests = 0
        fallback_dates: dict[str, set[date]] = {}
        batch: list[FireDetection] = []
        batch_keys: set[str] = set()
        request_errors: list[str] = []
        database_errors: list[str] = []
        batch_size = options['batch_size']

        def persist_batch() -> None:
            nonlocal rows_inserted, rows_would_insert, duplicates_skipped, failed_records
            if not batch:
                return

            keys = [item.record_key for item in batch]
            if options['dry_run']:
                existing = _database_keys(keys)
                duplicates_skipped += len(existing)
                rows_would_insert += len(keys) - len(existing)
            else:
                try:
                    with transaction.atomic():
                        before = _database_keys(keys)
                        FireDetection.objects.bulk_create(
                            batch,
                            ignore_conflicts=True,
                            batch_size=safe_sql_batch_size(
                                FireDetection, batch, batch_cap=batch_size,
                            ),
                        )
                        after = _database_keys(keys)
                    inserted = len(after - before)
                    rows_inserted += inserted
                    duplicates_skipped += len(keys) - inserted
                except Exception as exc:
                    failed_records += len(batch)
                    safe_error = str(exc).replace(map_key, '[redacted]')
                    database_errors.append(
                        f'Database batch failed and was rolled back: {safe_error}'
                    )
                    raise CommandError(database_errors[-1]) from exc
            batch.clear()
            batch_keys.clear()

        def add_detection(detection: FireDetection, request_seen: set[str]) -> None:
            nonlocal duplicates_skipped
            key = detection.record_key
            if key in request_seen or key in batch_keys:
                duplicates_skipped += 1
                return
            request_seen.add(key)
            batch.append(detection)
            batch_keys.add(key)
            if len(batch) >= batch_size:
                persist_batch()

        try:
            for dataset, sensor in products:
                days_requested = sorted(fallback_dates.get(dataset, set())) if dataset in fallbacks.values() else requested
                if not days_requested:
                    continue

                try:
                    available = _available_days(
                        fetch_availability_ranges(dataset, map_key),
                        days_requested,
                    )
                except Exception as exc:
                    failed_requests += 1
                    safe_error = str(exc).replace(map_key, '[redacted]')
                    request_errors.append(f'{dataset} availability: {safe_error}')
                    self.stderr.write(self.style.WARNING(f'{dataset}: availability check failed.'))
                    if dataset in fallbacks:
                        fallback_dates.setdefault(fallbacks[dataset], set()).update(days_requested)
                    continue

                if dataset in fallbacks:
                    unavailable = set(days_requested) - available
                    fallback_dates.setdefault(fallbacks[dataset], set()).update(unavailable)

                unavailable_count = len(set(days_requested) - available)
                if unavailable_count:
                    self.stdout.write(
                        f'{dataset}: {unavailable_count} requested date(s) are not available.'
                    )

                for segment_start, segment_end in _date_segments(available):
                    for chunk_start, day_range in date_chunks(segment_start, segment_end, max_days=1):
                        chunk_end = chunk_start + timedelta(days=day_range - 1)
                        self.stdout.write(
                            f'Fetching {dataset}: {chunk_start.isoformat()} to {chunk_end.isoformat()}'
                        )
                        try:
                            dataframe = fetch_dataset_chunk(
                                dataset, chunk_start, day_range, bbox, map_key,
                            )
                        except Exception as exc:
                            failed_requests += 1
                            safe_error = str(exc).replace(map_key, '[redacted]')
                            request_errors.append(f'{dataset} {chunk_start.isoformat()}: {safe_error}')
                            self.stderr.write(
                                self.style.WARNING(
                                    f'{dataset} {chunk_start.isoformat()}: request failed; continuing.'
                                )
                            )
                            continue

                        rows_downloaded += len(dataframe)
                        request_seen: set[str] = set()
                        for _, source_row in dataframe.iterrows():
                            row = source_row.to_dict()
                            try:
                                normalized = _normalise_firms_row(row, dataset, sensor)
                                detection = _row_to_fire_detection(normalized)
                                if detection is None:
                                    failed_records += 1
                                    continue
                                if (
                                    detection.acq_date != chunk_start
                                    or not math.isfinite(detection.latitude)
                                    or not math.isfinite(detection.longitude)
                                    or not -90 <= detection.latitude <= 90
                                    or not -180 <= detection.longitude <= 180
                                ):
                                    failed_records += 1
                                    continue
                                if geometry is not None and not point_in_geometry(
                                    detection.longitude, detection.latitude, geometry,
                                ):
                                    continue
                                add_detection(detection, request_seen)
                            except (ValidationError, TypeError, ValueError, OverflowError):
                                failed_records += 1

            persist_batch()
        except CommandError:
            pass
        except KeyboardInterrupt as exc:
            if not options['dry_run']:
                try:
                    persist_batch()
                except CommandError:
                    pass
            self.stdout.write(self.style.ERROR('Final status: INTERRUPTED'))
            raise CommandError('Import interrupted; completed batches remain committed.') from exc

        status = 'COMPLETED'
        if failed_requests or failed_records or database_errors:
            status = 'PARTIAL' if rows_inserted or rows_would_insert else 'FAILED'

        self.stdout.write(f'Records downloaded: {rows_downloaded}')
        self.stdout.write(f'Records inserted: {rows_inserted}')
        if options['dry_run']:
            self.stdout.write(f'Records that would be inserted: {rows_would_insert}')
        self.stdout.write(f'Duplicates skipped: {duplicates_skipped}')
        self.stdout.write(f'Failed records: {failed_records}')
        self.stdout.write(f'Failed requests: {failed_requests}')
        self.stdout.write(
            self.style.SUCCESS(f'Final status: {status}')
            if status == 'COMPLETED'
            else self.style.ERROR(f'Final status: {status}')
        )
        if request_errors:
            for error in request_errors:
                self.stderr.write(self.style.WARNING(error))
        if database_errors:
            for error in database_errors:
                self.stderr.write(self.style.ERROR(error))
        if status != 'COMPLETED':
            raise CommandError(
                f'FIRMS import {status.lower()}; review the failed records and requests above.'
            )
        if options['dry_run']:
            self.stdout.write('Dry run completed; no database records were saved.')
        elif rows_downloaded:
            self.stdout.write('FIRMS data ingestion completed and database writes were committed.')
        else:
            self.stdout.write('No FIRMS records were returned for the requested range.')

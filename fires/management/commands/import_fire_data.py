from __future__ import annotations

import csv
import hashlib
from datetime import datetime
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from fires.models import DataImportRun, FireDetection


def _first_present(row: dict, *keys: str) -> str | None:
    for key in keys:
        if key in row and row.get(key) not in (None, ''):
            return str(row.get(key)).strip()
    return None


def _parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if text in {'', 'nan', 'NaN', 'None', 'null', 'NULL'}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_date(value: str | None):
    if value is None:
        return None
    text = str(value).strip()
    if text in {'', 'nan', 'NaN', 'None', 'null', 'NULL'}:
        return None
    for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y'):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        return None


def _normalize_daynight(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text if text in {'D', 'N'} else None


def _build_record_key(row: dict) -> str:
    parts = [
        _first_present(row, 'source_product', 'sourceProduct') or '',
        _first_present(row, 'sensor', 'satellite') or '',
        _first_present(row, 'latitude', 'Latitude') or '',
        _first_present(row, 'longitude', 'Longitude') or '',
        _first_present(row, 'acq_date', 'acquisition_date') or '',
        _first_present(row, 'acq_time', 'acquisition_time') or '',
        _first_present(row, 'frp', 'FRP') or '',
        _first_present(row, 'brightness', 'Brightness') or '',
        _first_present(row, 'confidence_raw', 'confidence', 'confidence_category') or '',
        _first_present(row, 'daynight') or '',
        _first_present(row, 'satellite') or '',
        _first_present(row, 'instrument') or '',
        _first_present(row, 'version') or '',
    ]
    raw = '|'.join(str(part) for part in parts)
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def _row_to_fire_detection(row: dict) -> FireDetection | None:
    source_product = _first_present(row, 'source_product', 'sourceProduct', 'product')
    sensor = _first_present(row, 'sensor', 'sensor_name', 'source_product')
    latitude = _parse_float(_first_present(row, 'latitude', 'Latitude'))
    longitude = _parse_float(_first_present(row, 'longitude', 'Longitude'))
    acq_date = _parse_date(_first_present(row, 'acq_date', 'acquisition_date'))

    if not source_product or not sensor or latitude is None or longitude is None or acq_date is None:
        return None

    record_key = (row.get('record_key') or _first_present(row, 'record_key') or _build_record_key(row))
    if not record_key:
        record_key = _build_record_key(row)

    confidence_raw = _first_present(row, 'confidence_raw', 'confidence')
    confidence_category = _first_present(row, 'confidence_category', 'confidence_raw', 'confidence')
    if confidence_category is None and confidence_raw is not None:
        confidence_category = confidence_raw

    fire_detection = FireDetection(
        source_product=source_product,
        sensor=sensor,
        latitude=latitude,
        longitude=longitude,
        acq_date=acq_date,
        acq_time=_first_present(row, 'acq_time', 'acquisition_time'),
        brightness=_parse_float(_first_present(row, 'brightness', 'Brightness')),
        frp=_parse_float(_first_present(row, 'frp', 'FRP')),
        scan=_parse_float(_first_present(row, 'scan')),
        track=_parse_float(_first_present(row, 'track')),
        confidence_raw=(confidence_raw or None),
        confidence_category=(confidence_category or None),
        daynight=_normalize_daynight(_first_present(row, 'daynight')),
        satellite=_first_present(row, 'satellite'),
        instrument=_first_present(row, 'instrument'),
        version=_first_present(row, 'version'),
        record_key=str(record_key),
    )
    fire_detection.full_clean(validate_unique=False)
    return fire_detection


class Command(BaseCommand):
    help = 'Import processed FIRMS fire detections from the existing CSV export into the SQLite FireDetection tables.'

    def add_arguments(self, parser):
        parser.add_argument('--source', default='analysis/data/processed/fire_detections.csv', help='Path to the processed fire detections CSV.')
        parser.add_argument('--batch-size', type=int, default=1000, help='Number of rows to insert per batch.')

    def handle(self, *args, **options):
        source_path = Path(options['source'])
        if not source_path.is_absolute():
            source_path = Path(settings.BASE_DIR) / source_path

        if not source_path.exists():
            raise CommandError(
                f"Detection CSV not found: {source_path}. "
                "Run the existing analysis pipeline to generate analysis/data/processed/fire_detections.csv."
            )

        run = DataImportRun.objects.create(
            status='running',
            source_file=str(source_path),
            import_description='Import processed FIRMS fire detections into SQLite',
            started_at=timezone.now(),
        )

        rows_read = 0
        rows_inserted = 0
        rows_skipped = 0
        rows_failed = 0
        buffer: list[FireDetection] = []
        existing_keys = set(FireDetection.objects.values_list('record_key', flat=True))
        seen_keys: set[str] = set()

        try:
            with source_path.open('r', encoding='utf-8', newline='') as csv_file:
                reader = csv.DictReader(csv_file)
                if not reader.fieldnames:
                    raise CommandError(f"CSV file has no header row: {source_path}")

                for row in reader:
                    rows_read += 1
                    normalized = {key: (value.strip() if isinstance(value, str) else value) for key, value in row.items()}
                    detection = _row_to_fire_detection(normalized)
                    if detection is None:
                        rows_failed += 1
                        continue

                    key = detection.record_key
                    if key in existing_keys or key in seen_keys:
                        rows_skipped += 1
                        continue

                    buffer.append(detection)
                    existing_keys.add(key)
                    seen_keys.add(key)

                    if len(buffer) >= options['batch_size']:
                        with transaction.atomic():
                            created = FireDetection.objects.bulk_create(buffer, ignore_conflicts=True)
                        rows_inserted += len(created)
                        buffer.clear()

                if buffer:
                    with transaction.atomic():
                        created = FireDetection.objects.bulk_create(buffer, ignore_conflicts=True)
                    rows_inserted += len(created)
                    buffer.clear()

        except Exception as exc:
            run.status = 'failed'
            run.finished_at = timezone.now()
            run.rows_read = rows_read
            run.rows_inserted = rows_inserted
            run.rows_skipped = rows_skipped
            run.rows_failed = rows_failed
            run.error_message = str(exc)
            run.save(update_fields=['status', 'finished_at', 'rows_read', 'rows_inserted', 'rows_skipped', 'rows_failed', 'error_message'])
            raise CommandError(f'Import failed: {exc}') from exc

        run.status = 'completed'
        run.finished_at = timezone.now()
        run.rows_read = rows_read
        run.rows_inserted = rows_inserted
        run.rows_skipped = rows_skipped
        run.rows_failed = rows_failed
        run.save(update_fields=['status', 'finished_at', 'rows_read', 'rows_inserted', 'rows_skipped', 'rows_failed'])

        self.stdout.write(
            self.style.SUCCESS(
                f"Imported FireScope detection data: read={rows_read}, inserted={rows_inserted}, skipped={rows_skipped}, rejected={rows_failed}."
            )
        )

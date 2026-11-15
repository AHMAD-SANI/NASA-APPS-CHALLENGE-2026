from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import threading
import time
import uuid
from datetime import date, timedelta
from typing import Any

from django.core.exceptions import ValidationError
from django.db import close_old_connections, connections, transaction
from django.db.utils import OperationalError
from django.utils import timezone

from analysis.fetch_data import date_chunks, fetch_availability_ranges, fetch_dataset_chunk
from .batching import safe_in_batch_size, safe_sql_batch_size
from .geography import point_in_geometry, resolve_region
from .management.commands.import_fire_data import _build_record_key, _row_to_fire_detection
from .models import FireDetection, HistoricalJob, HistoricalJobDetection, LiveRetrievalRun
from .coverage import safe_coverage_error, scope_for_historical_job, upsert_coverage_records

logger = logging.getLogger(__name__)

_ALLOWED_SENSORS = {
    'MODIS',
    'VIIRS_SNPP_NRT',
    'VIIRS_NOAA20_NRT',
    'VIIRS_NOAA21_NRT',
    'MODIS_NRT',
    'VIIRS_SNPP',
    'VIIRS_NOAA20',
    'VIIRS_NOAA21',
    'MODIS_SP', 'MODIS_NRT',
    'VIIRS_SNPP_SP', 'VIIRS_SNPP_NRT',
    'VIIRS_NOAA20_SP', 'VIIRS_NOAA20_NRT',
    'VIIRS_NOAA21_SP', 'VIIRS_NOAA21_NRT',
}

_ACTIVE_STATUSES = ('queued', 'fetching', 'processing')
_FIRMS_DATASETS = {
    'MODIS': ('MODIS_SP', 'MODIS'),
    'MODIS_SP': ('MODIS_SP', 'MODIS'),
    'MODIS_NRT': ('MODIS_NRT', 'MODIS'),
    'VIIRS_SNPP': ('VIIRS_SNPP_SP', 'VIIRS_SNPP'),
    'VIIRS_SNPP_SP': ('VIIRS_SNPP_SP', 'VIIRS_SNPP'),
    'VIIRS_SNPP_NRT': ('VIIRS_SNPP_NRT', 'VIIRS_SNPP'),
    'VIIRS_NOAA20': ('VIIRS_NOAA20_SP', 'VIIRS_NOAA20'),
    'VIIRS_NOAA20_SP': ('VIIRS_NOAA20_SP', 'VIIRS_NOAA20'),
    'VIIRS_NOAA20_NRT': ('VIIRS_NOAA20_NRT', 'VIIRS_NOAA20'),
    'VIIRS_NOAA21': ('VIIRS_NOAA21_SP', 'VIIRS_NOAA21'),
    'VIIRS_NOAA21_SP': ('VIIRS_NOAA21_SP', 'VIIRS_NOAA21'),
    'VIIRS_NOAA21_NRT': ('VIIRS_NOAA21_NRT', 'VIIRS_NOAA21'),
}
_DEFAULT_FIRMS_SELECTIONS = ('MODIS', 'VIIRS_SNPP', 'VIIRS_NOAA20', 'VIIRS_NOAA21')
_FIRMS_PRODUCT_REGISTRY_VERSION = 3
# NRT files for the most recent UTC days keep growing; never treat them as final.
_UNSETTLED_DAYS = 2
_THREAD_LOCK = threading.Lock()
_JOB_CREATION_LOCK = threading.Lock()
_SQLITE_EXECUTION_LOCK = threading.Lock()
_THREAD_REGISTRY: dict[str, threading.Thread] = {}


def _normalise_sensor_names(raw_sensors: Any) -> list[str]:
    if raw_sensors is None:
        return []
    if isinstance(raw_sensors, str):
        return [raw_sensors.strip()]
    if not isinstance(raw_sensors, (list, tuple, set)):
        return []
    sensors = []
    for item in raw_sensors:
        if item is None:
            continue
        value = str(item).strip()
        if value and value not in sensors:
            sensors.append(value)
    return sensors


def _normalise_products(raw_products: Any) -> list[str]:
    return _normalise_sensor_names(raw_products)


def _hash_payload(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def _available_dates(ranges: list[tuple[date, date]], requested: list[date]) -> set[date]:
    return {
        day for day in requested
        if any(start <= day <= end for start, end in ranges)
    }


def _date_segments(days: set[date]) -> list[tuple[date, date]]:
    ordered = sorted(days)
    if not ordered:
        return []
    segments = []
    segment_start = segment_end = ordered[0]
    for day in ordered[1:]:
        if day == segment_end + timedelta(days=1):
            segment_end = day
        else:
            segments.append((segment_start, segment_end))
            segment_start = segment_end = day
    segments.append((segment_start, segment_end))
    return segments


def _unsettled_since() -> date:
    """First UTC date whose source files may still receive new detections."""
    return timezone.now().date() - timedelta(days=_UNSETTLED_DAYS - 1)


def _each_date(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _normalise_firms_row(row: dict[str, Any], dataset: str, sensor: str) -> dict[str, Any]:
    normalized = dict(row)
    normalized['source_product'] = dataset
    normalized['sensor'] = sensor
    brightness = normalized.get('brightness')
    if brightness in (None, '') or (isinstance(brightness, float) and math.isnan(brightness)):
        normalized['brightness'] = (
            normalized.get('bright_t31')
            or normalized.get('bright_ti4')
            or normalized.get('bright_ti5')
        )
    confidence = normalized.get('confidence_raw')
    if confidence in (None, '') or (isinstance(confidence, float) and math.isnan(confidence)):
        confidence = normalized.get('confidence')
    if isinstance(confidence, float) and math.isnan(confidence):
        confidence = None
    normalized['confidence_raw'] = confidence
    confidence_text = str(confidence).strip().lower() if confidence is not None else ''
    if confidence_text in {'n', 'l', 'h'}:
        normalized['confidence_category'] = confidence_text
    else:
        try:
            float(confidence_text)
            normalized['confidence_category'] = confidence_text
        except ValueError:
            normalized['confidence_category'] = None
    normalized['instrument'] = normalized.get('instrument') or ('MODIS' if dataset.startswith('MODIS') else 'VIIRS')
    normalized['record_key'] = _build_record_key(normalized)
    return normalized


class HistoricalJobService:
    THREAD_LIMIT = 2
    SQLITE_LOCK_RETRIES = 4

    @classmethod
    def recover_interrupted_jobs(cls):
        return HistoricalJob.objects.filter(status__in=_ACTIVE_STATUSES).update(
            status='interrupted',
            stage='interrupted',
            error_summary='Job left unfinished after a previous application process shutdown.',
            updated_at=timezone.now(),
        )

    @classmethod
    def create_or_get_job(cls, payload: dict[str, Any] | None) -> HistoricalJob:
        close_old_connections()
        data = payload or {}
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        if not start_date or not end_date:
            raise ValueError('Both start_date and end_date are required.')

        try:
            start = date.fromisoformat(str(start_date))
            end = date.fromisoformat(str(end_date))
        except ValueError as exc:
            raise ValueError('Dates must be valid ISO dates like YYYY-MM-DD.') from exc

        if end < start:
            raise ValueError('end_date must be on or after start_date.')

        days = (end - start).days + 1
        if days > 14610:
            raise ValueError('A single historical job is limited to 40 years. Split longer requests into smaller jobs.')

        sensor_list = _normalise_sensor_names(data.get('sensors'))
        product_list = _normalise_products(data.get('products'))
        if sensor_list:
            invalid = sorted(set(sensor_list) - _ALLOWED_SENSORS)
            if invalid:
                raise ValueError("Unsupported sensor names: " + ", ".join(invalid))
        if product_list:
            invalid = sorted(set(product_list) - _ALLOWED_SENSORS)
            if invalid:
                raise ValueError("Unsupported products: " + ", ".join(invalid))

        raw_region = data.get('region')
        resolved_region = None
        geometry = None
        if isinstance(raw_region, dict) and any(
            key in raw_region for key in ('country', 'type', 'region_type', 'name', 'region_name')
        ):
            if data.get('bbox'):
                raise ValueError('Provide either a named region or bbox, not both.')
            resolved_region, _ = resolve_region(raw_region)
            bbox = resolved_region['bbox']
        else:
            bbox = data.get('bbox') or raw_region or {}
            if not isinstance(bbox, dict):
                raise ValueError('bbox must be an object with south, west, north, and east values.')
        south = bbox.get('south')
        west = bbox.get('west')
        north = bbox.get('north')
        east = bbox.get('east')

        if any(value is not None for value in (south, west, north, east)):
            values = [south, west, north, east]
            if not all(value is not None for value in values):
                raise ValueError('A bounding box must include south, west, north, and east values.')
            try:
                south, west, north, east = (float(value) for value in values)
            except (TypeError, ValueError) as exc:
                raise ValueError('Bounding-box coordinates must be numeric.') from exc
            if not (-90 <= south <= 90 and -90 <= north <= 90 and -180 <= west <= 180 and -180 <= east <= 180):
                raise ValueError('Bounding-box coordinates must be within valid latitude and longitude ranges.')
            if south > north:
                raise ValueError('bbox south must be less than or equal to bbox north.')
            if west > east:
                raise ValueError('bbox west must be less than or equal to bbox east.')

        payload = {
            'start_date': start.isoformat(),
            'end_date': end.isoformat(),
            'sensors': sensor_list,
            'products': product_list,
            'bbox': {
                'south': south,
                'west': west,
                'north': north,
                'east': east,
            },
            'region': resolved_region,
        }
        request_hash = _hash_payload({
            **payload,
            'sensors': sorted(sensor_list),
            'products': sorted(product_list),
            'product_registry_version': _FIRMS_PRODUCT_REGISTRY_VERSION,
            **({'retry_nonce': str(data['retry_nonce'])} if data.get('retry_nonce') else {}),
        })

        with _JOB_CREATION_LOCK:
            reusable = (*_ACTIVE_STATUSES, 'completed') if end < _unsettled_since() else _ACTIVE_STATUSES
            existing = (
                HistoricalJob.objects.filter(request_hash=request_hash)
                .filter(status__in=reusable)
                .order_by('-started_at')
                .first()
            )
            if existing is not None:
                return existing

            job = HistoricalJob.objects.create(
                status='queued',
                stage='queued',
                request_hash=request_hash,
                request_params=payload,
                start_date=start,
                end_date=end,
                south=south,
                west=west,
                north=north,
                east=east,
                sensors=sensor_list,
                products=product_list,
                started_at=timezone.now(),
                updated_at=timezone.now(),
            )
            job_id = str(job.job_id)
            transaction.on_commit(lambda: cls.start_background_job(job_id))
        return job

    @classmethod
    def retry_job(cls, job: HistoricalJob) -> list[HistoricalJob]:
        unresolved = job.result_payload.get('missing_dates') or job.missing_dates or []
        if not unresolved and job.status in {'failed', 'interrupted'}:
            unresolved = [
                (job.start_date + timedelta(days=offset)).isoformat()
                for offset in range((job.end_date - job.start_date).days + 1)
            ]
        try:
            retry_dates = {date.fromisoformat(str(value)) for value in unresolved}
        except ValueError as exc:
            raise ValueError('The job does not contain valid retry dates.') from exc
        retry_dates = {day for day in retry_dates if job.start_date <= day <= job.end_date}
        if not retry_dates:
            return []

        retried = []
        for start, end in _date_segments(retry_dates):
            payload = {
                'start_date': start.isoformat(),
                'end_date': end.isoformat(),
                'sensors': job.sensors,
                'products': job.products,
                'bbox': job.request_params.get('bbox'),
                'region': job.request_params.get('region'),
                'retry_nonce': f'{job.job_id}:{uuid.uuid4()}',
            }
            retried.append(cls.create_or_get_job(payload))
        return retried

    @classmethod
    def start_background_job(cls, job_id: str):
        with _THREAD_LOCK:
            cls._start_background_job_locked(job_id)

    @classmethod
    def _start_background_job_locked(cls, job_id: str) -> bool:
        existing = _THREAD_REGISTRY.get(job_id)
        if existing is not None and existing.is_alive():
            return False

        active_count = sum(1 for thread in _THREAD_REGISTRY.values() if thread.is_alive())
        if active_count >= cls.THREAD_LIMIT:
            return False

        thread = threading.Thread(
            target=cls._run_job,
            args=(job_id,),
            name=f'historical-job-{job_id}',
            daemon=True,
        )
        _THREAD_REGISTRY[job_id] = thread
        try:
            thread.start()
        except Exception as exc:
            _THREAD_REGISTRY.pop(job_id, None)
            logger.exception('Could not start historical job %s.', job_id)
            cls._mark_job_failed(job_id, exc)
            return False
        return True

    @classmethod
    def _start_queued_jobs(cls):
        try:
            with _THREAD_LOCK:
                for job_id in HistoricalJob.objects.filter(status='queued').order_by(
                    'started_at'
                ).values_list('job_id', flat=True):
                    if sum(1 for thread in _THREAD_REGISTRY.values() if thread.is_alive()) >= cls.THREAD_LIMIT:
                        return
                    cls._start_background_job_locked(str(job_id))
        except Exception:
            logger.exception('Could not dispatch queued historical jobs.')

    @classmethod
    def _run_job(cls, job_id: str):
        try:
            execution_lock = _SQLITE_EXECUTION_LOCK if connections['default'].vendor == 'sqlite' else None
            if execution_lock is not None:
                execution_lock.acquire()
            try:
                for attempt in range(cls.SQLITE_LOCK_RETRIES + 1):
                    connections.close_all()
                    try:
                        cls._execute_job(job_id)
                        return
                    except OperationalError as exc:
                        if not cls._is_sqlite_lock_error(exc) or attempt == cls.SQLITE_LOCK_RETRIES:
                            raise
                        logger.warning(
                            'SQLite was busy while running historical job %s; retrying (%s/%s).',
                            job_id,
                            attempt + 1,
                            cls.SQLITE_LOCK_RETRIES,
                        )
                        connections.close_all()
                        time.sleep(0.05 * (2 ** attempt))
            finally:
                if execution_lock is not None:
                    execution_lock.release()
        except Exception as exc:
            logger.exception('Historical job %s failed.', job_id)
            cls._mark_job_failed(job_id, exc)
        finally:
            with _THREAD_LOCK:
                if _THREAD_REGISTRY.get(job_id) is threading.current_thread():
                    _THREAD_REGISTRY.pop(job_id, None)
            try:
                cls._start_queued_jobs()
            finally:
                connections.close_all()

    @classmethod
    def _execute_job(cls, job_id: str):
        job = HistoricalJob.objects.get(job_id=job_id)
        job.status = 'fetching'
        job.stage = 'fetching'
        job.progress = 10
        job.error_summary = ''
        job.updated_at = timezone.now()
        job.save(update_fields=['status', 'stage', 'progress', 'error_summary', 'updated_at'])

        result = cls._fetch_and_persist(job)
        requested_dates = result['requested_dates']
        covered_dates = result['covered_dates']
        missing_dates = result['missing_dates']
        all_covered = not missing_dates and not result['failures']
        any_covered = bool(covered_dates)
        job.records_processed = result['records_processed']
        job.actual_covered_dates = covered_dates
        job.missing_dates = missing_dates
        job.progress = 100
        job.finished_at = timezone.now()
        job.expires_at = timezone.now() + timedelta(hours=24)
        job.status = 'completed' if all_covered else 'partial' if any_covered else 'failed'
        job.stage = job.status
        job.error_summary = '; '.join(result['failures'])[:4000]
        job.result_payload = {
            'job_id': str(job.job_id),
            'status': job.status,
            'records_processed': result['records_processed'],
            'records_fetched': result['records_fetched'],
            'records_inserted': result['records_inserted'],
            'records_rejected': result['records_rejected'],
            'requested_dates': requested_dates,
            'actual_covered_dates': covered_dates,
            'missing_dates': missing_dates,
            'datasets': result['datasets'],
            'failures': result['failures'],
            'region': job.request_params.get('region'),
            'bbox': job.request_params.get('bbox'),
        }

        job.updated_at = timezone.now()
        job.save(update_fields=[
            'status', 'stage', 'records_processed', 'actual_covered_dates', 'missing_dates', 'progress',
            'error_summary', 'result_payload', 'finished_at', 'expires_at', 'updated_at'
        ])

    @classmethod
    def _fetch_and_persist(cls, job: HistoricalJob) -> dict[str, Any]:
        map_key = os.environ.get('FIRMS_MAP_KEY', '').strip()
        if not map_key:
            raise RuntimeError('FIRMS_MAP_KEY is not configured; historical FIRMS jobs cannot run.')

        selections = job.products or job.sensors or list(_DEFAULT_FIRMS_SELECTIONS)
        # A plain sensor selection (e.g. "MODIS") means "this sensor, best
        # available product": the standard archive (_SP) first, then the NRT
        # product for any dates the archive has not published yet (SP lags
        # NRT by a few months). Coverage is counted per sensor, not per product.
        dataset_specs = []
        seen_datasets = set()
        nrt_fallback = {}  # SP dataset -> NRT dataset that fills its gaps
        for selection in selections:
            dataset, sensor = _FIRMS_DATASETS[selection]
            if dataset not in seen_datasets:
                dataset_specs.append((dataset, sensor))
                seen_datasets.add(dataset)
            if selection == sensor and dataset.endswith('_SP'):
                nrt_dataset = dataset[:-3] + '_NRT'
                if nrt_dataset not in seen_datasets:
                    dataset_specs.append((nrt_dataset, sensor))
                    seen_datasets.add(nrt_dataset)
                    nrt_fallback[dataset] = nrt_dataset
        fallback_datasets = set(nrt_fallback.values())
        fallback_requested: dict[str, set[date]] = {}

        all_requested = []
        current = job.start_date
        while current <= job.end_date:
            all_requested.append(current)
            current += timedelta(days=1)
        requested_bbox = job.request_params.get('bbox') or {}
        bbox = requested_bbox if any(value is not None for value in requested_bbox.values()) else None
        geometry = None
        if job.request_params.get('region'):
            _, geometry = resolve_region(job.request_params['region'])

        covered_by_sensor: dict[str, set[date]] = {}
        failures = []
        datasets_result = []
        fetched_records = 0
        processed_records = 0
        inserted_records = 0
        rejected_records = 0
        scope_type, scope_identifier, scope_evidence = scope_for_historical_job(job)


        chunks_completed = 0
        for dataset_index, (dataset, sensor) in enumerate(dataset_specs):
            if dataset in fallback_datasets:
                requested = sorted(fallback_requested.get(dataset, set()))
                if not requested:
                    continue
            else:
                requested = list(all_requested)
            requested_set = set(requested)
            known_covered = cls._known_covered_dates(dataset, requested_set, bbox, geometry)
            cls._link_reused_detections(job, dataset, known_covered)
            try:
                ranges = fetch_availability_ranges(dataset, map_key)
                available = _available_dates(ranges, requested)
            except Exception as exc:
                failures.append(f'{dataset}: availability check failed: {exc}')
                attempted_at = timezone.now()
                upsert_coverage_records([{
                    'source_product': dataset, 'acquisition_date': day,
                    'scope_type': scope_type, 'scope_identifier': scope_identifier,
                    'availability_outcome': 'check_failed', 'retrieval_outcome': 'failed',
                    'observation_outcome': 'unknown', 'detection_count': None,
                    'rejected_count': None, 'retrieved_at': attempted_at, 'historical_job': job,
                    'attempt_key': f'historical:{job.job_id}:{dataset}:{day.isoformat()}',
                    'evidence': {'stage': 'availability_check', 'error': safe_coverage_error(exc, map_key),
                                 'request_scope': scope_evidence},
                } for day in requested])
                covered_by_sensor.setdefault(sensor, set())
                if dataset in nrt_fallback:
                    fallback_requested[nrt_fallback[dataset]] = set(requested)
                datasets_result.append({
                    'dataset': dataset, 'availability_status': 'unknown', 'available_dates': [],
                    'requested_dates': [day.isoformat() for day in requested],
                    'unavailable_dates': [], 'unknown_dates': [day.isoformat() for day in requested],
                    'retrieved_dates': [], 'detection_dates': [], 'zero_detection_dates': [],
                    'rejected_dates': [], 'failed_dates': [], 'pending_dates': [],
                    'available': False,
                })
                continue

            unavailable = requested_set - available
            if dataset in nrt_fallback:
                fallback_requested[nrt_fallback[dataset]] = set(unavailable)
            to_fetch = available - known_covered
            availability_at = timezone.now()
            upsert_coverage_records([{
                'source_product': dataset, 'acquisition_date': day,
                'scope_type': scope_type, 'scope_identifier': scope_identifier,
                'availability_outcome': 'unavailable' if day in unavailable else 'available',
                'retrieval_outcome': 'unavailable' if day in unavailable else 'unknown',
                'observation_outcome': 'unknown', 'detection_count': None,
                'rejected_count': None, 'retrieved_at': None, 'historical_job': job,
                'attempt_key': f'historical:{job.job_id}:{dataset}:{day.isoformat()}',
                'evidence': {'requested': True, 'available': day in available,
                             'previously_covered': day in known_covered,
                             'availability_checked_at': availability_at.isoformat(),
                             'request_scope': scope_evidence},
            } for day in requested])
            for segment_start, segment_end in ([] if dataset in nrt_fallback else _date_segments(unavailable)):
                failures.append(
                    f'{dataset}: archive unavailable from {segment_start.isoformat()} '
                    f'through {segment_end.isoformat()}.'
                )
            chunks = [
                chunk
                for segment_start, segment_end in _date_segments(to_fetch)
                for chunk in date_chunks(segment_start, segment_end)
            ]
            covered = set(known_covered)
            positive_dates: set[date] = set()
            zero_dates: set[date] = set()
            rejected_dates: set[date] = set()
            failed_dates: set[date] = set()
            for chunk_index, (chunk_start, day_range) in enumerate(chunks):
                chunk_end = chunk_start + timedelta(days=day_range - 1)
                try:
                    dataframe = fetch_dataset_chunk(dataset, chunk_start, day_range, bbox, map_key)
                    chunk_retrieved_at = timezone.now()
                    job.status = 'processing'
                    job.stage = 'processing'
                    job.progress = min(
                        95,
                        10 + int(80 * (dataset_index + (chunk_index + 1) / max(1, len(chunks))) / len(dataset_specs)),
                    )
                    job.updated_at = timezone.now()
                    job.save(update_fields=['status', 'stage', 'progress', 'updated_at'])
                    fetched_records += len(dataframe)
                    detections = []
                    rejected_in_chunk = 0
                    detection_counts: dict[date, int] = {}
                    rejected_counts: dict[date, int] = {}
                    unattributed_rejections = 0
                    for row in dataframe.to_dict(orient='records'):
                        try:
                            raw_day = date.fromisoformat(str(row.get('acq_date'))[:10])
                        except (TypeError, ValueError):
                            raw_day = None
                        normalized = _normalise_firms_row(row, dataset, sensor)
                        try:
                            detection = _row_to_fire_detection(normalized)
                        except (ValueError, TypeError, ValidationError):
                            rejected_in_chunk += 1
                            if raw_day in requested_set and chunk_start <= raw_day <= chunk_end:
                                rejected_counts[raw_day] = rejected_counts.get(raw_day, 0) + 1
                            else:
                                unattributed_rejections += 1
                            continue
                        if detection is None:
                            rejected_in_chunk += 1
                            if raw_day in requested_set and chunk_start <= raw_day <= chunk_end:
                                rejected_counts[raw_day] = rejected_counts.get(raw_day, 0) + 1
                            else:
                                unattributed_rejections += 1
                            continue
                        if (
                            detection.acq_date < chunk_start
                            or detection.acq_date > chunk_end
                            or detection.acq_date not in requested_set
                        ):
                            continue
                        if geometry is not None and not point_in_geometry(
                            detection.longitude, detection.latitude, geometry
                        ):
                            continue
                        detections.append(detection)
                        detection_counts[detection.acq_date] = detection_counts.get(detection.acq_date, 0) + 1
                    rejected_records += rejected_in_chunk
                    if rejected_in_chunk:
                        rejected_dates.update(rejected_counts)
                        if unattributed_rejections:
                            rejected_dates.update(
                                chunk_start + timedelta(days=offset) for offset in range(day_range)
                            )
                        failures.append(
                            f'{dataset} {chunk_start.isoformat()}: rejected {rejected_in_chunk} invalid detection row(s).'
                        )
                    processed_records += len(detections)
                    inserted_records += cls._persist_detections(job, detections)
                    chunk_detection_dates = {item.acq_date for item in detections}
                    positive_dates.update(chunk_detection_dates)
                    covered.update(
                        chunk_start + timedelta(days=offset) for offset in range(day_range)
                    )
                    zero_dates.update(
                        chunk_start + timedelta(days=offset)
                        for offset in range(day_range)
                        if chunk_start + timedelta(days=offset) not in chunk_detection_dates
                        and chunk_start + timedelta(days=offset) not in rejected_dates
                    )
                    upsert_coverage_records([{
                        'source_product': dataset, 'acquisition_date': day,
                        'scope_type': scope_type, 'scope_identifier': scope_identifier,
                        'availability_outcome': 'available',
                        'retrieval_outcome': 'partial' if rejected_counts.get(day, 0) or unattributed_rejections else 'succeeded',
                        'observation_outcome': (
                            'detections_returned' if detection_counts.get(day, 0)
                            else 'zero_detections' if not rejected_counts.get(day, 0) and not unattributed_rejections
                            else 'unknown'
                        ),
                        'detection_count': detection_counts.get(day, 0) if not unattributed_rejections else None,
                        'rejected_count': rejected_counts.get(day, 0) if not unattributed_rejections else None,
                        'retrieved_at': chunk_retrieved_at, 'historical_job': job,
                        'attempt_key': f'historical:{job.job_id}:{dataset}:{day.isoformat()}',
                        'evidence': {'validated_returned_detections': detection_counts.get(day, 0),
                                     'rejected_rows_for_date': rejected_counts.get(day, 0),
                                     'unattributed_rejected_rows_in_chunk': unattributed_rejections,
                                     'request_scope': scope_evidence},
                    } for day in _each_date(chunk_start, chunk_end)])
                except OperationalError as exc:
                    if cls._is_sqlite_lock_error(exc):
                        raise
                    failed_dates.update(chunk_start + timedelta(days=offset) for offset in range(day_range))
                    failures.append(
                        f'{dataset} {chunk_start.isoformat()} through {chunk_end.isoformat()}: {exc}'
                    )
                    upsert_coverage_records([{
                        'source_product': dataset, 'acquisition_date': day,
                        'scope_type': scope_type, 'scope_identifier': scope_identifier,
                        'availability_outcome': 'available', 'retrieval_outcome': 'failed',
                        'observation_outcome': 'unknown', 'detection_count': None,
                        'rejected_count': None, 'retrieved_at': timezone.now(), 'historical_job': job,
                        'attempt_key': f'historical:{job.job_id}:{dataset}:{day.isoformat()}',
                        'evidence': {'stage': 'fetch_parse_or_import', 'error': safe_coverage_error(exc, map_key),
                                     'request_scope': scope_evidence},
                    } for day in _each_date(chunk_start, chunk_end)])
                except Exception as exc:
                    failed_dates.update(chunk_start + timedelta(days=offset) for offset in range(day_range))
                    failures.append(
                        f'{dataset} {chunk_start.isoformat()} through {chunk_end.isoformat()}: {exc}'
                    )
                    upsert_coverage_records([{
                        'source_product': dataset, 'acquisition_date': day,
                        'scope_type': scope_type, 'scope_identifier': scope_identifier,
                        'availability_outcome': 'available', 'retrieval_outcome': 'failed',
                        'observation_outcome': 'unknown', 'detection_count': None,
                        'rejected_count': None, 'retrieved_at': timezone.now(), 'historical_job': job,
                        'attempt_key': f'historical:{job.job_id}:{dataset}:{day.isoformat()}',
                        'evidence': {'stage': 'fetch_parse_or_import', 'error': safe_coverage_error(exc, map_key),
                                     'request_scope': scope_evidence},
                    } for day in _each_date(chunk_start, chunk_end)])

            covered_by_sensor.setdefault(sensor, set()).update(covered)
            datasets_result.append({
                'dataset': dataset,
                'availability_status': 'available' if available else 'unavailable',
                'requested_dates': [day.isoformat() for day in requested],
                'available_dates': [day.isoformat() for day in sorted(available)],
                'reused_dates': [day.isoformat() for day in sorted(known_covered)],
                'unavailable_dates': [day.isoformat() for day in sorted(unavailable)],
                'unknown_dates': [],
                'retrieved_dates': [day.isoformat() for day in sorted(covered)],
                'detection_dates': [day.isoformat() for day in sorted(positive_dates)],
                'zero_detection_dates': [day.isoformat() for day in sorted(zero_dates)],
                'rejected_dates': [day.isoformat() for day in sorted(rejected_dates)],
                'failed_dates': [day.isoformat() for day in sorted(failed_dates)],
                'pending_dates': [day.isoformat() for day in sorted(available - covered - failed_dates)],
                'available': bool(covered),
            })

        requested = all_requested
        if covered_by_sensor:
            covered_dates = set.intersection(*covered_by_sensor.values())
        else:
            covered_dates = set()
        actual_dates = [day.isoformat() for day in sorted(covered_dates)]
        missing_dates = [day.isoformat() for day in requested if day not in covered_dates]
        processed_records = HistoricalJobDetection.objects.filter(job=job).count()
        return {
            'requested_dates': [day.isoformat() for day in requested],
            'covered_dates': actual_dates,
            'missing_dates': missing_dates,
            'records_fetched': fetched_records,
            'records_processed': processed_records,
            'records_inserted': inserted_records,
            'records_rejected': rejected_records,
            'datasets': datasets_result,
            'failures': failures,
        }

    @classmethod
    def _known_covered_dates(cls, dataset: str, requested: set[date], bbox, geometry) -> set[date]:
        """Coverage reuse requires an explicit successful retrieval ledger.

        Detection presence alone is not proof that a whole product/day was
        retrieved, so legacy/imported rows are deliberately not used here.
        Spatially restricted requests also fetch anew because ledger coverage
        is not region scoped.
        """
        if bbox or geometry is not None:
            return set()
        requested = {day for day in requested if day < _unsettled_since()}
        if not requested:
            return set()
        dates: set[date] = set()
        for run in LiveRetrievalRun.objects.filter(
            product=dataset, status__in=['succeeded', 'partial'],
        ).only('retrieved_dates'):
            for value in run.retrieved_dates:
                try:
                    day = date.fromisoformat(str(value))
                except (TypeError, ValueError):
                    continue
                if day in requested:
                    dates.add(day)
        for prior in HistoricalJob.objects.filter(
            status='completed', south__isnull=True, west__isnull=True,
            north__isnull=True, east__isnull=True,
        ).only('result_payload'):
            for entry in (prior.result_payload or {}).get('datasets', []):
                if entry.get('dataset') != dataset or entry.get('failed_dates') or entry.get('rejected_dates'):
                    continue
                for value in entry.get('retrieved_dates', []):
                    try:
                        day = date.fromisoformat(str(value))
                    except (TypeError, ValueError):
                        continue
                    if day in requested:
                        dates.add(day)
        return dates

    @classmethod
    def _link_reused_detections(cls, job: HistoricalJob, dataset: str, days: set[date]) -> None:
        """Attach already-stored detections for reused dates so job analytics include them."""
        if not days:
            return
        ids = FireDetection.objects.filter(
            source_product=dataset, acq_date__in=sorted(days),
        ).values_list('id', flat=True)
        batch_size = safe_in_batch_size()
        batch = []
        for detection_id in ids.iterator(chunk_size=batch_size):
            batch.append(HistoricalJobDetection(job=job, detection_id=detection_id))
            if len(batch) >= batch_size:
                HistoricalJobDetection.objects.bulk_create(batch, ignore_conflicts=True)
                batch = []
        if batch:
            HistoricalJobDetection.objects.bulk_create(batch, ignore_conflicts=True)

    @classmethod
    def _persist_detections(cls, job: HistoricalJob, detections: list[FireDetection]) -> int:
        inserted = 0
        batch_size = safe_in_batch_size()
        insert_batch_size = safe_sql_batch_size(FireDetection, detections)
        for offset in range(0, len(detections), batch_size):
            batch = detections[offset:offset + batch_size]
            keys = [detection.record_key for detection in batch]
            with transaction.atomic():
                before_count = FireDetection.objects.filter(record_key__in=keys).count()
                FireDetection.objects.bulk_create(
                    batch, ignore_conflicts=True, batch_size=insert_batch_size,
                )
                existing = list(FireDetection.objects.filter(record_key__in=keys))
                inserted += max(0, len(existing) - before_count)
                links = [
                    HistoricalJobDetection(job=job, detection=detection)
                    for detection in existing
                ]
                HistoricalJobDetection.objects.bulk_create(
                    links,
                    ignore_conflicts=True,
                    batch_size=safe_sql_batch_size(HistoricalJobDetection, links),
                )
        return inserted

    @classmethod
    def _is_sqlite_lock_error(cls, exc: OperationalError) -> bool:
        return connections['default'].vendor == 'sqlite' and any(
            marker in str(exc).lower() for marker in ('locked', 'busy')
        )

    @classmethod
    def _mark_job_failed(cls, job_id: str, exc: Exception):
        try:
            for attempt in range(cls.SQLITE_LOCK_RETRIES + 1):
                try:
                    job = HistoricalJob.objects.get(job_id=job_id)
                    job.status = 'failed'
                    job.stage = 'failed'
                    job.error_summary = str(exc)
                    job.finished_at = timezone.now()
                    job.updated_at = timezone.now()
                    job.save(update_fields=['status', 'stage', 'error_summary', 'finished_at', 'updated_at'])
                    return
                except OperationalError as db_exc:
                    if not cls._is_sqlite_lock_error(db_exc) or attempt == cls.SQLITE_LOCK_RETRIES:
                        raise
                    connections.close_all()
                    time.sleep(0.05 * (2 ** attempt))
        except HistoricalJob.DoesNotExist:
            logger.error('Cannot record failure for missing historical job %s: %s', job_id, exc)
        except Exception:
            logger.exception('Could not record failure for historical job %s.', job_id)

    @classmethod
    def wait_for_background_job(cls, job_id: str, timeout: float = 5.0) -> bool:
        with _THREAD_LOCK:
            thread = _THREAD_REGISTRY.get(str(job_id))
        if thread is None:
            return True
        thread.join(timeout)
        return not thread.is_alive()

    @classmethod
    def get_active_job_for_request(cls, payload: dict[str, Any]) -> HistoricalJob | None:
        close_old_connections()
        try:
            raw_region = payload.get('region')
            resolved_region = None
            if isinstance(raw_region, dict) and any(
                key in raw_region for key in ('country', 'type', 'region_type', 'name', 'region_name')
            ):
                resolved_region, _ = resolve_region(raw_region)
                bbox = resolved_region['bbox']
            else:
                bbox = payload.get('bbox') or raw_region or {}
            sensors = sorted(_normalise_sensor_names(payload.get('sensors')))
            products = sorted(_normalise_products(payload.get('products')))
            request_hash = _hash_payload({
                'start_date': date.fromisoformat(str(payload.get('start_date'))).isoformat(),
                'end_date': date.fromisoformat(str(payload.get('end_date'))).isoformat(),
                'sensors': sensors,
                'products': products,
                'product_registry_version': _FIRMS_PRODUCT_REGISTRY_VERSION,
                'bbox': {
                    'south': float(bbox['south']) if bbox.get('south') is not None else None,
                    'west': float(bbox['west']) if bbox.get('west') is not None else None,
                    'north': float(bbox['north']) if bbox.get('north') is not None else None,
                    'east': float(bbox['east']) if bbox.get('east') is not None else None,
                },
                'region': resolved_region,
            })
        except (TypeError, ValueError, KeyError):
            return None
        return (
            HistoricalJob.objects.filter(request_hash=request_hash)
            .filter(status__in=(*_ACTIVE_STATUSES, 'completed', 'partial'))
            .order_by('-started_at')
            .first()
        )

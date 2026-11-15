from __future__ import annotations

import os
import re
import threading
import time
import hashlib
import tempfile
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

from django.db import IntegrityError, OperationalError, close_old_connections, connections, transaction
from django.conf import settings
from django.utils import timezone

from analysis.fetch_data import date_chunks, fetch_availability_ranges, fetch_dataset_chunk, load_map_key
from .batching import safe_in_batch_size, safe_sql_batch_size
from .management.commands.import_fire_data import _row_to_fire_detection
from .models import FireDetection, LiveRetrievalRun
from .coverage import upsert_coverage_records
from .services import _date_segments, _normalise_firms_row

import logging

logger = logging.getLogger(__name__)

LIVE_PRODUCTS = {
    'MODIS_NRT': ('MODIS',),
    'VIIRS_SNPP_NRT': ('VIIRS_SNPP',),
    'VIIRS_NOAA20_NRT': ('VIIRS_NOAA20',),
    'VIIRS_NOAA21_NRT': ('VIIRS_NOAA21',),
}
_LIVE_LOCK = threading.Lock()
_LIVE_THREADS: dict[int, threading.Thread] = {}
_LIVE_THREAD_LIMIT = 4
_LIVE_RUN_LEASE = timedelta(hours=1)


@contextmanager
def _live_database_process_lock():
    """Serialize NRT writers across web and command processes for SQLite."""
    if connections['default'].vendor != 'sqlite':
        yield
        return
    database_name = str(settings.DATABASES['default']['NAME'])
    if database_name == ':memory:':
        yield
        return
    lock_id = hashlib.sha256(str(Path(database_name).resolve()).encode('utf-8')).hexdigest()[:20]
    lock_path = Path(tempfile.gettempdir()) / f'firescope-live-{lock_id}.lock'
    handle = lock_path.open('a+b')
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b'0')
            handle.flush()
        while True:
            try:
                if os.name == 'nt':
                    import msvcrt
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                time.sleep(0.25)
        try:
            yield
        finally:
            if os.name == 'nt':
                import msvcrt
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


def _live_thread_limit() -> int:
    """SQLite permits one writer at a time; serialize live product imports there."""
    return 1 if connections['default'].vendor == 'sqlite' else _LIVE_THREAD_LIMIT


def _persist_live_detection_batch(
    run: LiveRetrievalRun, detections: list[FireDetection], retrieved_at,
) -> int:
    """Persist one bounded idempotent batch in its own transaction."""
    keys = [detection.record_key for detection in detections]
    if not keys:
        return 0
    with transaction.atomic():
        before = FireDetection.objects.filter(record_key__in=keys).count()
        FireDetection.objects.bulk_create(
            detections,
            ignore_conflicts=True,
            batch_size=safe_sql_batch_size(FireDetection, detections),
        )
        after = FireDetection.objects.filter(record_key__in=keys).count()
        FireDetection.objects.filter(record_key__in=keys).update(retrieved_at=retrieved_at)
        inserted = max(0, after - before)
        run.rows_imported += inserted
        run.duplicates_skipped += len(detections) - inserted
        run.save(update_fields=['rows_imported', 'duplicates_skipped'])
    return inserted


def _safe_error(error: Exception, secret: str) -> str:
    value = str(error)
    if secret:
        value = value.replace(secret, '[redacted]')
    return re.sub(r'(api/area/csv/)[^/]+/', r'\1[redacted]/', value)[:2000]


def _expire_stale_runs() -> int:
    """Fail queued/running runs older than the lease, whatever their window.

    A process restart kills the worker thread but leaves the row active, which
    otherwise blocks the queue and makes the UI report a sync forever.
    """
    now = timezone.now()
    stale = LiveRetrievalRun.objects.filter(
        status__in=['queued', 'running'], requested_at__lt=now - _LIVE_RUN_LEASE,
    )
    # Status polls call this every minute. An UPDATE takes SQLite's write lock
    # even when it matches nothing, so read first and write only when needed.
    if not stale.exists():
        return 0
    return stale.update(
        status='failed', finished_at=now,
        error_summary='Refresh lease expired before completion (worker stopped or server restarted); a later refresh may retry this window.',
    )


class LiveRetrievalService:
    @classmethod
    def create_or_get_run(cls, product: str, window_days: int = 5) -> tuple[LiveRetrievalRun, bool]:
        if product not in LIVE_PRODUCTS:
            raise ValueError(f'Unsupported live product: {product}')
        try:
            window_days = int(window_days)
        except (TypeError, ValueError) as exc:
            raise ValueError('window_days must be an integer from 1 to 5.') from exc
        if not 1 <= window_days <= 5:
            raise ValueError('window_days must be between 1 and 5.')

        today = timezone.now().date()
        # Window identity is local to the refresh request. Availability and the
        # actual acquisition dates are resolved by the background worker.
        window_end = today
        window_start = today - timedelta(days=window_days - 1)
        sensor = LIVE_PRODUCTS[product][0]
        _expire_stale_runs()
        try:
            with transaction.atomic():
                run = LiveRetrievalRun.objects.create(
                    product=product,
                    sensor=sensor,
                    window_start=window_start,
                    window_end=window_end,
                    requested_dates=[day.isoformat() for day in _each_date(window_start, window_end)],
                )
            return run, True
        except IntegrityError:
            run = LiveRetrievalRun.objects.get(
                product=product,
                window_start=window_start,
                window_end=window_end,
                status__in=['queued', 'running'],
            )
            return run, False

    @classmethod
    def enqueue(
        cls, products: list[str] | None = None, window_days: int = 5, *, background: bool = True,
    ) -> list[LiveRetrievalRun]:
        selected = products if products is not None else list(LIVE_PRODUCTS)
        if any(not isinstance(product, str) for product in selected):
            raise ValueError('Each product must be an NRT product ID string.')
        unknown = sorted(set(selected) - set(LIVE_PRODUCTS))
        if unknown:
            raise ValueError('Unsupported live product(s): ' + ', '.join(unknown))
        runs = []
        for product in dict.fromkeys(selected):
            run, created = cls.create_or_get_run(product, window_days)
            runs.append(run)
            if created and background:
                transaction.on_commit(lambda run_id=run.pk: cls._start(run_id))
        return runs

    @classmethod
    def _start(cls, run_id: int) -> bool:
        with _LIVE_LOCK:
            thread = _LIVE_THREADS.get(run_id)
            if thread and thread.is_alive():
                return False
            if sum(1 for item in _LIVE_THREADS.values() if item.is_alive()) >= _live_thread_limit():
                return False
            thread = threading.Thread(
                target=cls.run,
                args=(run_id,),
                name=f'firms-live-{run_id}',
                daemon=True,
            )
            _LIVE_THREADS[run_id] = thread
            thread.start()
            return True

    @classmethod
    def _start_queued(cls) -> None:
        """Fill available worker slots from the durable queue."""
        for run_id in LiveRetrievalRun.objects.filter(status='queued').order_by('requested_at').values_list('pk', flat=True)[:_live_thread_limit()]:
            if not cls._start(run_id):
                break

    @classmethod
    def run(cls, run_id: int, dispatch_queued: bool = True) -> None:
        with _live_database_process_lock():
            cls._run_locked(run_id, dispatch_queued)

    @classmethod
    def _run_locked(cls, run_id: int, dispatch_queued: bool = True) -> None:
        close_old_connections()
        run = None
        key = ''
        availability_checked = False
        try:
            started_at = timezone.now()
            claimed = LiveRetrievalRun.objects.filter(pk=run_id, status='queued').update(
                status='running', started_at=started_at,
            )
            if not claimed:
                return
            run = LiveRetrievalRun.objects.get(pk=run_id)
            key = load_map_key()
            planned_dates = [date.fromisoformat(value) for value in (run.requested_dates or [])]
            ranges = fetch_availability_ranges(run.product, key)
            availability_checked = True
            today = timezone.now().date()
            source_dates = sorted({
                day for start, end in ranges
                for day in _each_date(start, min(end, today))
                if start <= today and day <= today
            })
            selected_dates = source_dates[-len(run.requested_dates):] if run.requested_dates else []
            run.available_dates = [day.isoformat() for day in selected_dates]
            run.requested_dates = list(run.available_dates)
            run.save(update_fields=['available_dates', 'requested_dates'])
            source_date_set = set(source_dates)
            coverage_rows = []
            for day in sorted(set(planned_dates) | set(selected_dates)):
                is_available = day in source_date_set
                selected = day in selected_dates
                coverage_rows.append({
                    'source_product': run.product, 'acquisition_date': day,
                    'scope_type': 'global', 'scope_identifier': 'world',
                    'availability_outcome': 'available' if is_available else 'unavailable',
                    'retrieval_outcome': 'unknown' if is_available else 'unavailable',
                    'observation_outcome': 'unknown', 'detection_count': None,
                    'rejected_count': None, 'retrieved_at': None,
                    'live_run': run, 'attempt_key': f'live:{run.pk}:{run.product}:{day.isoformat()}',
                    'evidence': {'requested': day in planned_dates, 'available': is_available,
                                 'selected_for_retrieval': selected, 'scope': 'global'},
                })
            upsert_coverage_records(coverage_rows)
            if not selected_dates:
                raise RuntimeError(f'FIRMS reports no available dates for {run.product}.')
            successful_dates: set[str] = set()
            detection_dates: set[str] = set()
            rejected_dates: set[str] = set()
            failed_dates: set[str] = set()
            # The selected NRT window is at most five calendar days.
            chunks = [
                chunk
                for segment_start, segment_end in _date_segments(set(selected_dates))
                for chunk in date_chunks(segment_start, segment_end)
            ]
            for chunk_start, days in chunks:
                chunk_end = chunk_start + timedelta(days=days - 1)
                try:
                    frame = fetch_dataset_chunk(
                        run.product, chunk_start, days, None, key,
                    )
                    chunk_retrieved_at = timezone.now()
                    run.rows_fetched += len(frame)
                    # Preserve the fetched count even if validation or a later
                    # database batch fails.
                    run.save(update_fields=['rows_fetched'])
                    detections = []
                    rejected = 0
                    chunk_detection_dates: set[str] = set()
                    detection_counts: dict[date, int] = {}
                    rejected_counts: dict[date, int] = {}
                    unattributed_rejections = 0
                    for raw in frame.to_dict(orient='records'):
                        try:
                            raw_day = date.fromisoformat(str(raw.get('acq_date'))[:10])
                        except (TypeError, ValueError):
                            raw_day = None
                        normalized = _normalise_firms_row(raw, run.product, run.sensor)
                        try:
                            detection = _row_to_fire_detection(normalized)
                        except Exception:
                            rejected += 1
                            if raw_day in set(_each_date(chunk_start, chunk_end)):
                                rejected_counts[raw_day] = rejected_counts.get(raw_day, 0) + 1
                            else:
                                unattributed_rejections += 1
                            continue
                        if detection is None or not (
                            chunk_start <= detection.acq_date <= chunk_end
                            and -90 <= detection.latitude <= 90
                            and -180 <= detection.longitude <= 180
                        ):
                            rejected += 1
                            if raw_day in set(_each_date(chunk_start, chunk_end)):
                                rejected_counts[raw_day] = rejected_counts.get(raw_day, 0) + 1
                            else:
                                unattributed_rejections += 1
                            continue
                        detection.retrieved_at = chunk_retrieved_at
                        detections.append(detection)
                        detection_counts[detection.acq_date] = detection_counts.get(detection.acq_date, 0) + 1
                        chunk_detection_dates.add(detection.acq_date.isoformat())
                    run.rows_rejected += rejected
                    # Only dates that actually had rejected rows are marked; rows
                    # without a usable date taint the whole chunk.
                    rejected_dates.update(day.isoformat() for day in rejected_counts)
                    if unattributed_rejections:
                        rejected_dates.update(day.isoformat() for day in _each_date(chunk_start, chunk_end))
                    run.save(update_fields=['rows_rejected'])

                    # Keep identity and duplicate handling intact while limiting
                    # each IN query, INSERT and transaction to a safe batch.
                    unique_by_key = {item.record_key: item for item in detections}
                    unique_detections = list(unique_by_key.values())
                    run.duplicates_skipped += len(detections) - len(unique_detections)
                    if len(detections) != len(unique_detections):
                        run.save(update_fields=['duplicates_skipped'])
                    batch_size = safe_in_batch_size()
                    for offset in range(0, len(unique_detections), batch_size):
                        batch = unique_detections[offset:offset + batch_size]
                        _persist_live_detection_batch(run, batch, chunk_retrieved_at)
                    successful_dates.update(
                        day.isoformat() for day in _each_date(chunk_start, chunk_end)
                    )
                    detection_dates.update(chunk_detection_dates)
                    run.retrieved_dates = sorted(successful_dates)
                    run.detection_dates = sorted(detection_dates)
                    run.rejected_dates = sorted(rejected_dates)
                    run.zero_detection_dates = sorted(successful_dates - detection_dates - rejected_dates)
                    run.failed_dates = sorted(failed_dates)
                    run.save(update_fields=[
                        'rows_fetched', 'rows_imported', 'duplicates_skipped', 'rows_rejected',
                        'retrieved_dates', 'detection_dates', 'zero_detection_dates', 'rejected_dates', 'failed_dates',
                    ])
                    upsert_coverage_records([{
                        'source_product': run.product, 'acquisition_date': day,
                        'scope_type': 'global', 'scope_identifier': 'world',
                        'availability_outcome': 'available',
                        'retrieval_outcome': 'partial' if rejected_counts.get(day, 0) or unattributed_rejections else 'succeeded',
                        'observation_outcome': (
                            'detections_returned' if detection_counts.get(day, 0)
                            else 'zero_detections' if not rejected_counts.get(day, 0) and not unattributed_rejections
                            else 'unknown'
                        ),
                        'detection_count': detection_counts.get(day, 0) if not unattributed_rejections else None,
                        'rejected_count': rejected_counts.get(day, 0) if not unattributed_rejections else None,
                        'retrieved_at': chunk_retrieved_at, 'live_run': run,
                        'attempt_key': f'live:{run.pk}:{run.product}:{day.isoformat()}',
                        'evidence': {'validated_returned_detections': detection_counts.get(day, 0),
                                     'rejected_rows_for_date': rejected_counts.get(day, 0),
                                     'unattributed_rejected_rows_in_chunk': unattributed_rejections},
                    } for day in _each_date(chunk_start, chunk_end)])
                except Exception as exc:
                    failed_dates.update(day.isoformat() for day in _each_date(chunk_start, chunk_end))
                    run.failed_dates = sorted(failed_dates)
                    message = _safe_error(exc, key)
                    run.error_summary = (run.error_summary + '; ' + message).strip('; ')[:2000]
                    run.save(update_fields=['failed_dates', 'error_summary'])
                    upsert_coverage_records([{
                        'source_product': run.product, 'acquisition_date': day,
                        'scope_type': 'global', 'scope_identifier': 'world',
                        'availability_outcome': 'available', 'retrieval_outcome': 'failed',
                        'observation_outcome': 'unknown', 'detection_count': None,
                        'rejected_count': None, 'retrieved_at': timezone.now(), 'live_run': run,
                        'attempt_key': f'live:{run.pk}:{run.product}:{day.isoformat()}',
                        'evidence': {'error': _safe_error(exc, key), 'stage': 'fetch_parse_or_import'},
                    } for day in _each_date(chunk_start, chunk_end)])

            run.finished_at = timezone.now()
            run.last_success_at = run.finished_at if successful_dates else None
            run.latest_acquisition_date = max(
                (date.fromisoformat(day) for day in detection_dates), default=None,
            )
            run.status = (
                'failed' if not successful_dates
                else 'partial' if failed_dates or run.rows_rejected > 0
                else 'succeeded'
            )
            run.save(update_fields=[
                'status', 'finished_at', 'last_success_at', 'latest_acquisition_date',
                'error_summary', 'failed_dates',
            ])
        except Exception as exc:
            if run is not None:
                run.status = 'failed'
                run.finished_at = timezone.now()
                run.error_summary = _safe_error(exc, key)
                run.save(update_fields=['status', 'finished_at', 'error_summary'])
                if run.pk and not availability_checked:
                    planned = [date.fromisoformat(value) for value in (run.requested_dates or [])]
                    upsert_coverage_records([{
                        'source_product': run.product, 'acquisition_date': day,
                        'scope_type': 'global', 'scope_identifier': 'world',
                        'availability_outcome': 'check_failed', 'retrieval_outcome': 'failed',
                        'observation_outcome': 'unknown', 'detection_count': None,
                        'rejected_count': None, 'retrieved_at': run.finished_at, 'live_run': run,
                        'attempt_key': f'live:{run.pk}:{run.product}:{day.isoformat()}',
                        'evidence': {'error': _safe_error(exc, key), 'stage': 'availability_check'},
                    } for day in planned])
        finally:
            with _LIVE_LOCK:
                _LIVE_THREADS.pop(run_id, None)
            close_old_connections()
            if dispatch_queued:
                cls._start_queued()

    @classmethod
    def status(cls) -> dict:
        stale_hours = int(os.getenv('FIRESCOPE_LIVE_STALE_HOURS', '24'))
        # Status is polled by the UI: clear dead runs and resume the durable
        # queue so syncing cannot stay stuck after a restart.
        try:
            _expire_stale_runs()
            cls._start_queued()
        except OperationalError:
            # A long import holds SQLite's write lock; housekeeping can wait
            # for the next poll rather than failing the status read.
            logger.warning('Database busy; skipped live-run housekeeping this poll.')
        now = timezone.now()
        products = []
        for product, (sensor,) in LIVE_PRODUCTS.items():
            runs = LiveRetrievalRun.objects.filter(product=product)
            latest = runs.order_by('-requested_at').only(
                'pk', 'status', 'requested_at', 'available_dates', 'retrieved_dates',
                'detection_dates', 'zero_detection_dates', 'rejected_dates', 'failed_dates',
                'rows_imported', 'duplicates_skipped', 'error_summary',
            ).first()
            active_run_stale = bool(
                latest
                and latest.status in {'queued', 'running'}
                and latest.requested_at < now - _LIVE_RUN_LEASE
            )
            last_success = runs.filter(last_success_at__isnull=False).order_by('-last_success_at').only('last_success_at').first()
            latest_acquisition_date = runs.filter(
                latest_acquisition_date__isnull=False,
            ).order_by('-latest_acquisition_date').values_list(
                'latest_acquisition_date', flat=True,
            ).first()
            age_hours = (
                (now - last_success.last_success_at).total_seconds() / 3600
                if last_success else None
            )
            products.append({
                'product': product,
                'sensor': sensor,
                'latest_acquisition_date': latest_acquisition_date.isoformat() if latest_acquisition_date else None,
                'last_successful_retrieval_at': last_success.last_success_at.isoformat()
                if last_success else None,
                'latest_outcome': latest.status if latest else 'never_retrieved',
                'available_dates': latest.available_dates if latest else [],
                'retrieved_dates': latest.retrieved_dates if latest else [],
                'detection_dates': latest.detection_dates if latest else [],
                'zero_detection_dates': latest.zero_detection_dates if latest else [],
                'rejected_dates': latest.rejected_dates if latest else [],
                'failed_dates': latest.failed_dates if latest else [],
                'newly_imported_records': latest.rows_imported if latest else 0,
                'duplicate_records_skipped': latest.duplicates_skipped if latest else 0,
                'error': latest.error_summary if latest and latest.status in {'failed', 'partial'} else None,
                'active_run_id': latest.pk if latest and latest.status in {'queued', 'running'} else None,
                'active_run_stale': active_run_stale,
                'stale': age_hours is None or age_hours > stale_hours,
                'stale_after_hours': stale_hours,
            })
        return {'generated_at': now.isoformat(), 'products': products}


def _each_date(start: date, end: date):
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)


def _chunks(start: date, end: date):
    day = start
    while day <= end:
        size = min(5, (end - day).days + 1)
        yield day, size
        day += timedelta(days=size)

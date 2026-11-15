"""Keep the live FIRMS feed current while the backend server is running."""
from __future__ import annotations

import logging
import os
import shutil
import sys
import threading
import time
from datetime import timedelta
from pathlib import Path

from django.conf import settings
from django.db import close_old_connections, connection
from django.utils import timezone

from fires.live import LIVE_PRODUCTS, LiveRetrievalService, _expire_stale_runs
from fires.models import LiveRetrievalRun

logger = logging.getLogger(__name__)

STARTUP_DELAY_SECONDS = 30
CHECK_SECONDS = 300
DEFAULT_INTERVAL_HOURS = 3.0
MIN_WINDOW_DAYS = 2  # The newest day keeps gaining overpasses, so re-fetch it.
MAX_WINDOW_DAYS = 5
# Only the current UTC day is pulled unless FIRESCOPE_LIVE_WINDOW_DAYS asks
# for catch-up of missed days (up to MAX_WINDOW_DAYS).
DEFAULT_WINDOW_DAYS = 1
MIN_FREE_BYTES = 3 * 1024**3

_started = False
_started_lock = threading.Lock()


def interval_hours() -> float:
    """Hours between refreshes of a product. Zero or less disables the scheduler."""
    try:
        return float(os.getenv('FIRESCOPE_LIVE_REFRESH_HOURS', DEFAULT_INTERVAL_HOURS))
    except ValueError:
        return DEFAULT_INTERVAL_HOURS


def max_window_days() -> int:
    """Most days one scheduled refresh reaches back, counting today."""
    try:
        days = int(os.getenv('FIRESCOPE_LIVE_WINDOW_DAYS', DEFAULT_WINDOW_DAYS))
    except ValueError:
        days = DEFAULT_WINDOW_DAYS
    return max(1, min(MAX_WINDOW_DAYS, days))


def due_refreshes(now=None) -> dict[str, int]:
    """Return {product: window_days} for products that should refresh now."""
    now = now or timezone.now()
    interval = timedelta(hours=interval_hours())
    cap = max_window_days()
    floor = min(MIN_WINDOW_DAYS, cap)
    due = {}
    for product in LIVE_PRODUCTS:
        runs = LiveRetrievalRun.objects.filter(product=product)
        if runs.filter(status__in=['queued', 'running']).exists():
            continue
        # Spacing is measured from the last attempt, so failures are not hammered.
        latest = runs.order_by('-requested_at').first()
        if latest and now - latest.requested_at < interval:
            continue
        success = runs.filter(status='succeeded').order_by('-window_end').first()
        days_behind = (now.date() - success.window_end).days + 1 if success else cap
        due[product] = max(floor, min(cap, days_behind))
    return due


def has_disk_space() -> bool:
    database = Path(settings.DATABASES['default']['NAME'])
    return shutil.disk_usage(database.parent).free >= MIN_FREE_BYTES


def checkpoint() -> None:
    """Fold SQLite's journal into the database and shrink it.

    Constant dashboard reads can stop SQLite's automatic checkpoint from ever
    finishing, and the journal then grows until the disk is full.
    """
    if connection.vendor == 'sqlite':
        with connection.cursor() as cursor:
            cursor.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            cursor.fetchone()


def tick() -> dict[str, int]:
    _expire_stale_runs()
    if not LiveRetrievalRun.objects.filter(status__in=['queued', 'running']).exists():
        checkpoint()
    if not has_disk_space():
        logger.warning('Skipping FIRMS refresh: less than %d GB free on the database drive.', MIN_FREE_BYTES // 1024**3)
        return {}
    # Queued runs lose their worker thread when the server restarts.
    LiveRetrievalService._start_queued()
    due = due_refreshes()
    for product, window_days in due.items():
        LiveRetrievalService.enqueue([product], window_days=window_days)
    return due


def _loop() -> None:
    time.sleep(STARTUP_DELAY_SECONDS)
    while True:
        close_old_connections()
        try:
            due = tick()
            if due:
                logger.info('Queued scheduled FIRMS refresh: %s', due)
        except Exception:
            # One bad cycle must not end automatic refreshing.
            logger.exception('Scheduled FIRMS refresh check failed')
        finally:
            close_old_connections()
        time.sleep(CHECK_SECONDS)


def should_run(argv=None, environ=None) -> bool:
    argv = sys.argv if argv is None else argv
    environ = os.environ if environ is None else environ
    if environ.get('FIRESCOPE_LIVE_SCHEDULER') == '1':
        return True  # Explicit opt-in for servers other than runserver.
    if 'runserver' not in argv:
        return False  # Tests and management commands must never call NASA.
    # runserver's autoreloader has a parent process that serves no requests.
    return environ.get('RUN_MAIN') == 'true' or '--noreload' in argv


def start() -> bool:
    global _started
    if interval_hours() <= 0 or not should_run():
        return False
    with _started_lock:
        if _started:
            return False
        _started = True
    threading.Thread(target=_loop, name='firms-live-scheduler', daemon=True).start()
    return True

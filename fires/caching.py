"""Version-keyed result caching for expensive dashboard queries."""
from __future__ import annotations

import hashlib
import json
import threading

from django.core.cache import cache
from django.db.models import Count, Max

_LOCKS: dict[str, threading.Lock] = {}
_LOCKS_GUARD = threading.Lock()


def data_version() -> str:
    """Cheap fingerprint that changes whenever detections or coverage change."""
    from .models import CoverageLedger, FireDetection, HistoricalJobDetection

    detections = FireDetection.objects.aggregate(max_id=Max('id'), latest=Max('retrieved_at'))
    coverage = CoverageLedger.objects.aggregate(max_id=Max('id'), latest=Max('retrieved_at'), count=Count('id'))
    links = HistoricalJobDetection.objects.aggregate(max_id=Max('id'))
    return json.dumps([detections, coverage, links], default=str, sort_keys=True)


def cache_key(prefix: str, parts) -> str:
    return prefix + ':' + hashlib.sha256(json.dumps(parts, default=str).encode()).hexdigest()


def single_flight(key: str, compute, timeout: int = 600):
    """Return the cached value for key, computing it at most once at a time.

    The dashboard fires several identical heavy requests at once (and again
    on reload); without the per-key lock each one scanned the same millions
    of SQLite rows concurrently and they starved each other into timeouts.
    """
    value = cache.get(key)
    if value is not None:
        return value
    with _LOCKS_GUARD:
        lock = _LOCKS.setdefault(key, threading.Lock())
    try:
        with lock:
            value = cache.get(key)
            if value is None:
                value = compute()
                cache.set(key, value, timeout=timeout)
            return value
    finally:
        with _LOCKS_GUARD:
            if _LOCKS.get(key) is lock:
                del _LOCKS[key]

"""Bounded helpers for persisting per-attempt product/date coverage evidence."""

import json
import re

from .models import CoverageLedger


_UPDATE_FIELDS = (
    'scope_type', 'scope_identifier', 'availability_outcome', 'retrieval_outcome',
    'observation_outcome', 'detection_count', 'rejected_count', 'retrieved_at',
    'live_run', 'historical_job', 'evidence',
)


def scope_for_historical_job(job):
    params = job.request_params or {}
    region = params.get('region')
    bbox = params.get('bbox') or {}
    if region:
        identifier = ':'.join(str(region.get(key, '') or '') for key in (
            'country_code', 'type', 'boundary_code', 'name',
        ))
        return 'region', identifier, {'region': region, 'bbox': bbox}
    if any(value is not None for value in bbox.values()):
        return 'bbox', json.dumps(bbox, sort_keys=True, separators=(',', ':')), {'bbox': bbox}
    return 'global', 'world', {'bbox': None, 'region': None}


def safe_coverage_error(error, secret=''):
    value = str(error)
    if secret:
        value = value.replace(secret, '[redacted]')
    return re.sub(r'(api/area/csv/)[^/]+/', r'\1[redacted]/', value)[:2000]


def upsert_coverage_records(records, batch_size=50):
    """Insert or update attempts by their stable attempt_key in bounded SQL batches."""
    rows = list(records)
    for offset in range(0, len(rows), batch_size):
        batch = rows[offset:offset + batch_size]
        keys = [record['attempt_key'] for record in batch]
        existing = {
            row.attempt_key: row
            for row in CoverageLedger.objects.filter(attempt_key__in=keys)
        }
        to_create = []
        to_update = []
        for record in batch:
            current = existing.get(record['attempt_key'])
            if current is None:
                to_create.append(CoverageLedger(**record))
                continue
            for field in _UPDATE_FIELDS:
                if field in record:
                    setattr(current, field, record[field])
            to_update.append(current)
        if to_create:
            CoverageLedger.objects.bulk_create(
                to_create, ignore_conflicts=True, batch_size=batch_size,
            )
        if to_update:
            CoverageLedger.objects.bulk_update(
                to_update, _UPDATE_FIELDS, batch_size=batch_size,
            )

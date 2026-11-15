"""Coverage-aware product/date aggregation with scope-preserving statistics."""

from calendar import monthrange
from datetime import date, timedelta
import json
import math
import statistics

from django.db import connection
from django.db.models import Count, OuterRef, Q, Subquery

from .models import CoverageLedger
from .geography import point_in_geometry
from .caching import cache_key, data_version, single_flight


PRODUCT_SENSORS = {
    'MODIS_NRT': 'MODIS', 'MODIS_SP': 'MODIS',
    'VIIRS_SNPP_NRT': 'VIIRS_SNPP', 'VIIRS_SNPP_SP': 'VIIRS_SNPP',
    'VIIRS_NOAA20_NRT': 'VIIRS_NOAA20', 'VIIRS_NOAA20_SP': 'VIIRS_NOAA20',
    'VIIRS_NOAA21_NRT': 'VIIRS_NOAA21', 'VIIRS_NOAA21_SP': 'VIIRS_NOAA21',
}
PRODUCTS = tuple(PRODUCT_SENSORS)


def date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def resolve_scope_filter(scope: str | None, scope_identifier: str | None):
    selected = (scope or 'global').strip()
    identifier = (scope_identifier or '').strip()
    scope_types = dict(CoverageLedger.SCOPE_CHOICES)
    if selected not in scope_types:
        if identifier:
            raise ValueError('Use either scope or scope_identifier, not both.')
        known = set(CoverageLedger.objects.filter(
            scope_identifier=selected,
        ).values_list('scope_type', flat=True).distinct()[:3])
        if len(known) > 1:
            raise ValueError('This scope identifier is ambiguous; include a scope type.')
        return (next(iter(known)) if known else 'unknown'), selected
    if selected in {'region', 'bbox'} and not identifier:
        raise ValueError(f'scope_identifier is required when scope={selected}.')
    if selected == 'bbox':
        try:
            bbox = json.loads(identifier)
            if not isinstance(bbox, dict) or set(bbox) != {'south', 'west', 'north', 'east'}:
                raise ValueError
            south, west, north, east = (float(bbox[key]) for key in ('south', 'west', 'north', 'east'))
            if not all(math.isfinite(value) for value in (south, west, north, east)):
                raise ValueError
            if not (-90 <= south <= 90 and -90 <= north <= 90 and -180 <= west <= 180 and -180 <= east <= 180):
                raise ValueError
            if south > north or west > east:
                raise ValueError
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError('scope_identifier for bbox must be JSON with valid south, west, north, and east coordinates.') from exc
        identifier = json.dumps(
            {'south': south, 'west': west, 'north': north, 'east': east},
            sort_keys=True, separators=(',', ':'),
        )
    if selected == 'global':
        return selected, identifier or 'world'
    if selected == 'unknown':
        return selected, identifier or 'observations-only'
    return selected, identifier


def _scope_area(scope_type, scope_identifier):
    """Return (bbox, polygon) for region/bbox scopes, else None."""
    if scope_type == 'bbox':
        return json.loads(scope_identifier), None
    if scope_type == 'region':
        from .geography import get_region_details
        parts = scope_identifier.split(':', 3)
        if len(parts) != 4:
            return None
        country_code, region_type, _, region_name = parts
        try:
            details = get_region_details(country_code, region_type, region_name)
        except (ValueError, RuntimeError):
            return None
        return details['bbox'], details.get('polygon')
    return None


def _coverage_queryset(start, end, products, scope_type, scope_identifier):
    return CoverageLedger.objects.filter(
        source_product__in=products,
        acquisition_date__range=(start, end),
        scope_type=scope_type,
        scope_identifier=scope_identifier,
    )


def _attempt_groups(start, end, products, scope_type, scope_identifier):
    latest = CoverageLedger.objects.filter(
        source_product=OuterRef('source_product'),
        acquisition_date=OuterRef('acquisition_date'),
        scope_type=OuterRef('scope_type'),
        scope_identifier=OuterRef('scope_identifier'),
    ).order_by('-created_at', '-id')
    return list(
        _coverage_queryset(start, end, products, scope_type, scope_identifier)
        .values('source_product', 'acquisition_date', 'scope_type', 'scope_identifier')
        .annotate(
            attempt_count=Count('id'),
            succeeded_count=Count('id', filter=Q(retrieval_outcome='succeeded')),
            partial_count=Count('id', filter=Q(retrieval_outcome='partial')),
            failed_count=Count('id', filter=Q(retrieval_outcome='failed')),
            unavailable_count=Count('id', filter=Q(retrieval_outcome='unavailable')),
            available_count=Count('id', filter=Q(availability_outcome='available')),
            availability_unavailable_count=Count('id', filter=Q(availability_outcome='unavailable')),
            availability_failed_count=Count('id', filter=Q(availability_outcome='check_failed')),
            positive_count=Count('id', filter=Q(observation_outcome='detections_returned')),
            zero_count=Count('id', filter=Q(observation_outcome='zero_detections')),
            latest_attempt_key=Subquery(latest.values('attempt_key')[:1]),
            latest_retrieved_at=Subquery(latest.values('retrieved_at')[:1]),
            latest_detection_count=Subquery(latest.values('detection_count')[:1]),
            latest_rejected_count=Subquery(latest.values('rejected_count')[:1]),
        )
    )


def coverage_state(group):
    # A successful result survives a later failed retry. Attempts remain
    # visible in the counts and latest-attempt reference below.
    if group['succeeded_count']:
        if group['positive_count']:
            return 'detections_returned'
        if group['zero_count']:
            return 'confirmed_zero_detections'
        if group['partial_count']:
            return 'incomplete'
        return 'unknown'
    if group['partial_count']:
        return 'incomplete'
    if group['failed_count']:
        return 'failed'
    if group['unavailable_count']:
        return 'unavailable'
    return 'unknown'


def _coverage_by_date(start, end, products, scope_type, scope_identifier):
    by_date = _own_coverage_by_date(start, end, products, scope_type, scope_identifier)
    if scope_type in {'region', 'bbox'} and _scope_area(scope_type, scope_identifier):
        # A worldwide retrieval also covers this area. Dates without an
        # area-specific attempt inherit it; whether the area itself had
        # detections comes from the stored rows inside its boundary.
        positive = {
            (row['source_product'], date.fromisoformat(row['period']))
            for row in _cached_scoped_statistics(start, end, products, scope_type, scope_identifier)['day']
        }
        world = _own_coverage_by_date(start, end, products, 'global', 'world')
        for key, entry in world.items():
            if key in by_date:
                continue
            inherited = {**entry, 'scope_type': scope_type, 'scope_identifier': scope_identifier,
                         'inherited_from': 'global/world'}
            if entry['coverage_state'] in {'detections_returned', 'confirmed_zero_detections'}:
                found = key in positive
                inherited['coverage_state'] = 'detections_returned' if found else 'confirmed_zero_detections'
                inherited['observation_outcome'] = 'detections_returned' if found else 'zero_detections'
            by_date[key] = inherited
    return by_date


def _own_coverage_by_date(start, end, products, scope_type, scope_identifier):
    by_date = {}
    for group in _attempt_groups(start, end, products, scope_type, scope_identifier):
        key = (group['source_product'], group['acquisition_date'])
        by_date[key] = {
            'coverage_state': coverage_state(group),
            'retrieval_outcome': (
                'succeeded' if group['succeeded_count'] else
                'partial' if group['partial_count'] else
                'failed' if group['failed_count'] else
                'unavailable' if group['unavailable_count'] else 'unknown'
            ),
            'availability_outcome': (
                'available' if group['available_count'] else
                'unavailable' if group['availability_unavailable_count'] else
                'check_failed' if group['availability_failed_count'] else 'unknown'
            ),
            'scope_type': group['scope_type'],
            'scope_identifier': group['scope_identifier'],
            'retrieval_outcome_counts': {
                'succeeded': group['succeeded_count'], 'partial': group['partial_count'],
                'failed': group['failed_count'], 'unavailable': group['unavailable_count'],
            },
            'observation_outcome': (
                'detections_returned' if group['positive_count'] else
                'zero_detections' if group['zero_count'] else 'unknown'
            ),
            'attempt_count': group['attempt_count'],
            'latest_attempt_key': group['latest_attempt_key'],
            'latest_retrieved_at': group['latest_retrieved_at'].isoformat()
            if group['latest_retrieved_at'] else None,
            'latest_returned_detection_count': group['latest_detection_count'],
            'latest_rejected_count': group['latest_rejected_count'],
        }
    return by_date


def _scoped_observation_aggregates(start, end, products, scope_type, scope_identifier, period):
    return _cached_scoped_statistics(start, end, products, scope_type, scope_identifier)[period]


def _cached_scoped_statistics(start, end, products, scope_type, scope_identifier):
    """Share one detection scan between the daily and monthly endpoints."""
    key = cache_key('scoped-stats', [
        start.isoformat(), end.isoformat(), list(products), scope_type, scope_identifier, data_version(),
    ])
    return single_flight(key, lambda: _scoped_statistics(start, end, products, scope_type, scope_identifier))


def _scoped_statistics(start, end, products, scope_type, scope_identifier, periods=('day', 'month')):
    """Aggregate stored detections after assigning each row to one explicit scope.

    Global rows are live observations (retrieved_at) or observations linked to
    a global historical job. Regional/bbox rows are job-linked observations.
    Unlinked legacy imports go to the explicit unknown scope. UNION removes
    retry/job-link duplication before any statistical aggregate is calculated.
    """
    # Use Django's portable DB-API placeholder so DEBUG's query renderer can
    # interpolate parameters consistently (SQLite's raw '?' breaks it).
    placeholders = ', '.join('%s' for _ in products)
    base = f'fd.source_product IN ({placeholders}) AND fd.acq_date BETWEEN %s AND %s'
    base_params = (*products, start.isoformat(), end.isoformat())
    fd = connection.ops.quote_name('fires_firedetection')
    hd = connection.ops.quote_name('fires_historicaljobdetection')
    cl = connection.ops.quote_name('fires_coverageledger')
    # Each stored detection is emitted at most once, so no UNION/DISTINCT
    # de-duplication pass over millions of rows is needed:
    #  * global/world: live rows, plus non-live rows linked to a global job;
    #  * region/bbox: rows linked to a job with that exact scope;
    #  * unknown: non-live rows not linked to any job coverage.
    # "+fd.retrieved_at" keeps SQLite on the product/date covering index
    # instead of the low-selectivity retrieved_at index.
    linked_to_scope = f'''EXISTS (
                SELECT 1 FROM {hd} hd
                JOIN {cl} cl ON cl.historical_job_id = hd.job_id
                    AND cl.source_product = fd.source_product
                    AND cl.acquisition_date = fd.acq_date
                WHERE hd.detection_id = fd.id
                  AND cl.scope_type = %s AND cl.scope_identifier = %s
            )'''
    linked_to_any = f'''EXISTS (
                SELECT 1 FROM {hd} hd
                JOIN {cl} cl ON cl.historical_job_id = hd.job_id
                    AND cl.source_product = fd.source_product
                    AND cl.acquisition_date = fd.acq_date
                WHERE hd.detection_id = fd.id
            )'''
    if scope_type == 'global' and scope_identifier == 'world':
        condition = f'(+fd.retrieved_at IS NOT NULL OR {linked_to_scope})'
        condition_params = (scope_type, scope_identifier)
    elif scope_type == 'unknown' and scope_identifier == 'observations-only':
        condition = f'+fd.retrieved_at IS NULL AND NOT {linked_to_any}'
        condition_params = ()
    else:
        condition = linked_to_scope
        condition_params = (scope_type, scope_identifier)
    area = _scope_area(scope_type, scope_identifier) if scope_type in {'region', 'bbox'} else None
    polygon = None
    if area:
        # Worldwide rows (live, or linked to a global job) inside the area
        # count too: a global retrieval covers every area within it.
        bounds, polygon = area
        # Rows linked to a job for this exact area were scoped by that job.
        condition = (
            f'({linked_to_scope} OR (fd.latitude BETWEEN %s AND %s AND fd.longitude BETWEEN %s AND %s '
            f'AND (+fd.retrieved_at IS NOT NULL OR {linked_to_scope})))'
        )
        condition_params = (
            scope_type, scope_identifier,
            bounds['south'], bounds['north'], bounds['west'], bounds['east'],
            'global', 'world',
        )
        own_column = f'CASE WHEN {linked_to_scope} THEN 1 ELSE 0 END'
        own_params = (scope_type, scope_identifier)
    else:
        own_column, own_params = '1', ()
    branch_params = (*own_params, *base_params, *condition_params)
    sql = f'''
        SELECT fd.source_product, fd.acq_date, fd.frp, fd.daynight, fd.latitude, fd.longitude,
               {own_column}
        FROM {fd} fd
        WHERE {base} AND {condition}
    '''
    # One indexed scan feeds both daily and monthly statistics. Exact medians
    # are computed here; SQLite window functions needed a full sort per call
    # and dominated request time on large live datasets.
    groups: dict[str, dict[tuple, dict]] = {name: {} for name in periods}
    with connection.cursor() as cursor:
        cursor.execute(sql, branch_params)
        while True:
            chunk = cursor.fetchmany(20000)
            if not chunk:
                break
            for product, acq_date, frp, daynight, latitude, longitude, own in chunk:
                if polygon and not own and not point_in_geometry(longitude, latitude, polygon):
                    continue
                day_text = acq_date.isoformat() if isinstance(acq_date, date) else str(acq_date)[:10]
                flag = (daynight or '').upper()
                for name, by_key in groups.items():
                    period_key = day_text if name == 'day' else day_text[:7]
                    group = by_key.get((product, period_key))
                    if group is None:
                        group = by_key[(product, period_key)] = {
                            'count': 0, 'frp': [], 'day': 0, 'night': 0, 'missing': 0, 'dates': set(),
                        }
                    group['count'] += 1
                    group['dates'].add(day_text)
                    if frp is None:
                        group['missing'] += 1
                    else:
                        group['frp'].append(frp)
                    if flag == 'D':
                        group['day'] += 1
                    elif flag == 'N':
                        group['night'] += 1
    return {name: _statistics_rows(by_key, scope_type, scope_identifier) for name, by_key in groups.items()}


def _statistics_rows(groups, scope_type, scope_identifier):
    rows = []
    for (product, period_key), group in groups.items():
        values = group['frp']
        rows.append({
            'source_product': product, 'period': period_key,
            'scope_type': scope_type, 'scope_identifier': scope_identifier,
            'detection_count': group['count'],
            'mean_frp': sum(values) / len(values) if values else None,
            'median_frp': statistics.median(values) if values else None,
            'maximum_frp': max(values) if values else None,
            'daytime_count': group['day'], 'nighttime_count': group['night'],
            'missing_frp_count': group['missing'],
            'dates_with_detections': len(group['dates']),
        })
    return rows


def daily_aggregates(start, end, products, scope_type, scope_identifier):
    coverage = _coverage_by_date(start, end, products, scope_type, scope_identifier)
    stats = {
        (row['source_product'], row['period'] if isinstance(row['period'], date) else date.fromisoformat(row['period'])): row
        for row in _scoped_observation_aggregates(start, end, products, scope_type, scope_identifier, 'day')
    }
    rows = []
    for product in products:
        for day in date_range(start, end):
            key = (product, day)
            stat = stats.get(key)
            attempt = coverage.get(key)
            state = attempt['coverage_state'] if attempt else 'unknown'
            if stat:
                values = {
                    'detection_count': stat['detection_count'],
                    'mean_frp': stat['mean_frp'], 'median_frp': stat['median_frp'],
                    'maximum_frp': stat['maximum_frp'],
                    'daytime_detection_count': stat['daytime_count'],
                    'nighttime_detection_count': stat['nighttime_count'],
                    'missing_frp_count': stat['missing_frp_count'],
                }
                if not attempt:
                    state = 'detections_returned'
                retrieval = attempt or {
                    'scope_type': scope_type, 'scope_identifier': scope_identifier,
                    'coverage_state': state, 'retrieval_outcome_counts': {
                        'succeeded': 0, 'partial': 0, 'failed': 0, 'unavailable': 0,
                    }, 'retrieval_outcome': 'unknown', 'availability_outcome': 'unknown',
                    'observation_outcome': 'detections_returned', 'attempt_count': 0,
                    'latest_attempt_key': None, 'latest_retrieved_at': None,
                    'latest_returned_detection_count': None, 'latest_rejected_count': None,
                }
            elif state == 'confirmed_zero_detections':
                values = {
                    'detection_count': 0, 'mean_frp': None, 'median_frp': None,
                    'maximum_frp': None, 'daytime_detection_count': 0,
                    'nighttime_detection_count': 0, 'missing_frp_count': 0,
                }
                retrieval = attempt
            else:
                values = {
                    'detection_count': None, 'mean_frp': None, 'median_frp': None,
                    'maximum_frp': None, 'daytime_detection_count': None,
                    'nighttime_detection_count': None, 'missing_frp_count': None,
                }
                retrieval = attempt or {
                    'scope_type': scope_type, 'scope_identifier': scope_identifier,
                    'coverage_state': 'unknown', 'retrieval_outcome_counts': {
                        'succeeded': 0, 'partial': 0, 'failed': 0, 'unavailable': 0,
                    }, 'retrieval_outcome': 'unknown', 'availability_outcome': 'unknown',
                    'observation_outcome': 'unknown', 'attempt_count': 0,
                    'latest_attempt_key': None, 'latest_retrieved_at': None,
                    'latest_returned_detection_count': None, 'latest_rejected_count': None,
                }
            rows.append({
                'source_product': product, 'sensor': PRODUCT_SENSORS[product],
                'acquisition_date': day.isoformat(), 'scope_type': scope_type,
                'scope_identifier': scope_identifier, 'coverage_state': state,
                'retrieval_outcome': retrieval['retrieval_outcome'],
                'availability_outcome': retrieval['availability_outcome'],
                'observation_outcome': 'detections_returned' if stat else retrieval['observation_outcome'],
                **values, 'retrieval_evidence': retrieval,
                'statistics_complete_for_scope': (
                    retrieval['retrieval_outcome'] == 'succeeded'
                    and retrieval['observation_outcome'] in {'detections_returned', 'zero_detections'}
                ),
                'statistics_basis': 'distinct stored detections assigned to the requested scope',
            })
    return rows


def monthly_aggregates(start, end, products, scope_type, scope_identifier):
    coverage = _coverage_by_date(start, end, products, scope_type, scope_identifier)
    scoped = _cached_scoped_statistics(start, end, products, scope_type, scope_identifier)
    daily_stats = {
        (row['source_product'], date.fromisoformat(row['period'])): row
        for row in scoped['day']
    }
    monthly_stats = {(row['source_product'], row['period']): row for row in scoped['month']}
    rows = []
    current = date(start.year, start.month, 1)
    end_month = date(end.year, end.month, 1)
    while current <= end_month:
        month_end = date(current.year, current.month, monthrange(current.year, current.month)[1])
        first, last = max(start, current), min(end, month_end)
        denominator = (last - first).days + 1
        month_text = current.strftime('%Y-%m')
        for product in products:
            state_counts = {
                'detections_returned_dates': 0, 'confirmed_zero_dates': 0,
                'unavailable_dates': 0, 'failed_dates': 0,
                'unknown_dates': 0, 'incomplete_dates': 0,
                'dates_with_failed_attempts': 0, 'dates_with_partial_attempts': 0,
                'dates_with_unavailable_attempts': 0,
            }
            eligible_counts = []
            for day in date_range(first, last):
                attempt = coverage.get((product, day))
                state = attempt['coverage_state'] if attempt else 'unknown'
                if attempt:
                    outcome_counts = attempt['retrieval_outcome_counts']
                    state_counts['dates_with_failed_attempts'] += int(outcome_counts['failed'] > 0)
                    state_counts['dates_with_partial_attempts'] += int(outcome_counts['partial'] > 0)
                    state_counts['dates_with_unavailable_attempts'] += int(outcome_counts['unavailable'] > 0)
                counter = {
                    'detections_returned': 'detections_returned_dates',
                    'confirmed_zero_detections': 'confirmed_zero_dates',
                    'unavailable': 'unavailable_dates', 'failed': 'failed_dates',
                    'incomplete': 'incomplete_dates', 'unknown': 'unknown_dates',
                }.get(state, 'unknown_dates')
                state_counts[counter] += 1
                if state == 'confirmed_zero_detections':
                    eligible_counts.append(0)
                elif state == 'detections_returned':
                    daily = daily_stats.get((product, day))
                    # Without stored detections for this scope the day's count is
                    # unknown, so it must not lower the mean as a zero.
                    if daily:
                        eligible_counts.append(daily['detection_count'])
            stat = monthly_stats.get((product, month_text))
            fully_confirmed_zero = state_counts['confirmed_zero_dates'] == denominator
            positive_days = sum(
                1 for day in date_range(first, last)
                if (product, day) in daily_stats
            )
            rows.append({
                'source_product': product, 'sensor': PRODUCT_SENSORS[product],
                'month': month_text, 'scope_type': scope_type,
                'scope_identifier': scope_identifier,
                'total_detections': stat['detection_count'] if stat else 0 if fully_confirmed_zero else None,
                'daily_mean_detection_count': (
                    sum(eligible_counts) / len(eligible_counts) if eligible_counts else None
                ),
                'eligible_observed_dates': len(eligible_counts),
                'total_detections_complete_for_scope': len(eligible_counts) == denominator,
                'frp_statistics_complete_for_scope': len(eligible_counts) == denominator,
                'mean_frp': stat['mean_frp'] if stat else None,
                'median_frp': stat['median_frp'] if stat else None,
                'maximum_frp': stat['maximum_frp'] if stat else None,
                'daytime_detection_count': stat['daytime_count'] if stat else 0 if fully_confirmed_zero else None,
                'nighttime_detection_count': stat['nighttime_count'] if stat else 0 if fully_confirmed_zero else None,
                'dates_with_detections': positive_days if stat else 0 if fully_confirmed_zero else None,
                'missing_frp_count': stat['missing_frp_count'] if stat else 0 if fully_confirmed_zero else None,
                'coverage_denominator_days': denominator,
                **state_counts,
                'statistics_basis': 'distinct stored detections assigned to the requested scope',
            })
        current = date(current.year + (current.month == 12), 1 if current.month == 12 else current.month + 1, 1)
    return rows


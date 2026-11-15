from __future__ import annotations

from pathlib import Path
import os
import hashlib
import json

from django.conf import settings
from django.core.cache import cache
from django.db.models import Max
from django.db import connection
from django.db.models import Avg, Count, F, Func, Q, Sum, Value
from django.db.models.functions import Floor
from django.http import FileResponse
from django.shortcuts import get_object_or_404
from django.utils import timezone
from django.utils.dateparse import parse_date
from rest_framework.decorators import api_view, throttle_classes
from rest_framework.exceptions import ValidationError
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.throttling import ScopedRateThrottle

from .geography import get_region_details, list_administrative_levels, list_countries, list_regions, point_in_geometry
from .live import LIVE_PRODUCTS, LiveRetrievalService
from .models import CoverageLedger, FireDetection, HistoricalJob, HistoricalJobDetection
from .coverage import safe_coverage_error
from .aggregation import (
    PRODUCTS as AGGREGATION_PRODUCTS,
    PRODUCT_SENSORS as AGGREGATION_PRODUCT_SENSORS,
    daily_aggregates,
    monthly_aggregates,
    resolve_scope_filter,
)
from . import harmonized
from . import archive_prepare
from .services import HistoricalJobService
from .batching import max_sql_parameters
from .caching import cache_key, data_version, single_flight

PROCESSED_DIR = Path(settings.BASE_DIR) / 'analysis' / 'data' / 'processed'
CHARTS_DIR = PROCESSED_DIR / 'charts'


def _resolve_geo_scope(request):
    """Return (bounds, polygon) for geo_scope query params; (None, None) for global."""
    geo_scope = request.query_params.get('geo_scope', 'global') or 'global'
    identifier = request.query_params.get('geo_scope_identifier', '')
    if geo_scope == 'global':
        return None, None
    if geo_scope == 'bbox':
        try:
            _, normalized = resolve_scope_filter('bbox', identifier)
        except ValueError as exc:
            raise ValidationError({'geo_scope_identifier': str(exc)})
        return json.loads(normalized), None
    if geo_scope == 'region':
        parts = identifier.split(':', 3)
        if len(parts) != 4:
            raise ValidationError({'geo_scope_identifier': 'Region scope identifier must include country, level, boundary ID and name.'})
        country_code, region_type, boundary_id, region_name = parts
        try:
            details = get_region_details(country_code, region_type, region_name)
        except (ValueError, RuntimeError) as exc:
            raise ValidationError({'geo_scope_identifier': str(exc)})
        if boundary_id and details.get('boundary_code') != boundary_id:
            raise ValidationError({'geo_scope_identifier': 'Region boundary identifier no longer matches the selected boundary.'})
        return details['bbox'], details.get('polygon')
    raise ValidationError({'geo_scope': 'geo_scope must be global, bbox, or region.'})


def _apply_geo_scope(queryset, request):
    bounds, polygon = _resolve_geo_scope(request)
    if bounds is None:
        return queryset
    queryset = queryset.filter(
        latitude__gte=bounds['south'], latitude__lte=bounds['north'],
        longitude__gte=bounds['west'], longitude__lte=bounds['east'],
    )
    if not polygon:
        return queryset
    ids = [
        row['id'] for row in queryset.order_by().values('id', 'latitude', 'longitude').iterator(chunk_size=5000)
        if point_in_geometry(row['longitude'], row['latitude'], polygon)
    ]
    if len(ids) > max_sql_parameters() - 50:
        raise ValidationError({'detail': 'Too many detections in this region for one request; narrow the date range or filters.'})
    return queryset.filter(id__in=ids)


def _apply_filters(queryset, request, order_by=None, *, geo=True):
    if geo:
        queryset = _apply_geo_scope(queryset, request)
    scope = request.query_params.get('scope', 'all')
    if scope == 'live':
        queryset = queryset.filter(source_product__in=LIVE_PRODUCTS.keys(), retrieved_at__isnull=False)
    elif scope == 'historical':
        queryset = queryset.filter(retrieved_at__isnull=True)

    sensor = request.query_params.get('sensor')
    if sensor:
        queryset = queryset.filter(sensor__iexact=sensor)

    source_product = request.query_params.get('source_product')
    if source_product:
        queryset = queryset.filter(source_product__iexact=source_product)

    daynight = request.query_params.get('daynight')
    if daynight:
        queryset = queryset.filter(daynight__iexact=daynight)

    confidence = request.query_params.get('confidence_category')
    if confidence:
        queryset = queryset.filter(confidence_category__iexact=confidence)

    for param, lookup in (('start_date', 'acq_date__gte'), ('end_date', 'acq_date__lte')):
        raw = request.query_params.get(param)
        if raw:
            try:
                value = parse_date(raw)
            except ValueError:
                value = None
            if value is None:
                raise ValidationError({param: 'Use a valid date in YYYY-MM-DD format.'})
            queryset = queryset.filter(**{lookup: value})

    try:
        minimum_frp = float(request.query_params.get('min_frp', 0))
    except (TypeError, ValueError):
        minimum_frp = 0
    if minimum_frp > 0:
        queryset = queryset.filter(frp__gte=minimum_frp)

    confidence_filter = request.query_params.get('confidence_filter')
    if confidence_filter == 'high':
        queryset = queryset.filter(confidence_category__in=['h', 'high', 'H', 'HIGH'])
    elif confidence_filter == 'nominal_high':
        queryset = queryset.exclude(confidence_category__in=['l', 'low', 'L', 'LOW'])

    return queryset.order_by(*(order_by or ('-acq_date', '-id')))


def _serialize_detection_row(row: dict) -> dict:
    return {
        'id': row.get('id'),
        'source_product': row.get('source_product'),
        'sensor': row.get('sensor'),
        'latitude': row.get('latitude'),
        'longitude': row.get('longitude'),
        'acq_date': row.get('acq_date'),
        'acq_time': row.get('acq_time'),
        'brightness': row.get('brightness'),
        'frp': row.get('frp'),
        'scan': row.get('scan'),
        'track': row.get('track'),
        'confidence_raw': row.get('confidence_raw'),
        'confidence_category': row.get('confidence_category'),
        'daynight': row.get('daynight'),
        'satellite': row.get('satellite'),
        'instrument': row.get('instrument'),
        'version': row.get('version'),
        'record_key': row.get('record_key'),
        'retrieved_at': row.get('retrieved_at'),
    }


@api_view(['GET'])
def health(request):
    return Response({
        'ok': True,
        'project': 'FireScope',
        'generated_at': timezone.now().isoformat(),
        'status': 'ready',
    })


class CoveragePagination(PageNumberPagination):
    page_size = 50
    page_size_query_param = 'page_size'
    max_page_size = 100


class AggregationPagination(PageNumberPagination):
    page_size = 50
    page_size_query_param = 'page_size'
    max_page_size = 100


def _aggregation_request(request, *, monthly=False):
    start_raw = request.query_params.get('start_date')
    end_raw = request.query_params.get('end_date')
    start = parse_date(start_raw) if start_raw else None
    end = parse_date(end_raw) if end_raw else None
    if not start or not end:
        return None, Response({'detail': 'start_date and end_date are required in YYYY-MM-DD format.'}, status=400)
    if start > end:
        return None, Response({'detail': 'start_date must be on or before end_date.'}, status=400)
    if monthly:
        month_span = (end.year - start.year) * 12 + (end.month - start.month)
        if month_span >= 12:
            return None, Response({'detail': 'Monthly aggregation is limited to 12 calendar months per request.'}, status=400)
    elif (end - start).days + 1 > 366:
        return None, Response({'detail': 'Daily aggregation is limited to 366 dates per request.'}, status=400)

    requested_product = request.query_params.get('source_product')
    if requested_product:
        product = requested_product.strip().upper()
        if product not in AGGREGATION_PRODUCT_SENSORS:
            return None, Response({'detail': 'Unsupported source_product.'}, status=400)
        products = [product]
    else:
        products = list(AGGREGATION_PRODUCTS)
        if not monthly and (end - start).days + 1 > 90:
            return None, Response({
                'detail': 'Daily ranges over 90 dates require a source_product filter.',
            }, status=400)
    sensor = request.query_params.get('sensor')
    if sensor:
        selected_sensor = sensor.strip().upper()
        products = [p for p in products if AGGREGATION_PRODUCT_SENSORS[p].upper() == selected_sensor]
        if not products:
            return None, Response({'detail': 'sensor does not match the selected source_product.'}, status=400)
    try:
        scope_type, scope_identifier = resolve_scope_filter(
            request.query_params.get('scope'), request.query_params.get('scope_identifier'),
        )
    except ValueError as exc:
        return None, Response({'detail': str(exc)}, status=400)
    return (start, end, products, scope_type, scope_identifier), None


def _cached_rows(kind, args, compute):
    """Compute aggregate rows once per request shape and data version.

    Paginated clients request several pages of the same result; without this
    every page re-ran the full aggregation.
    """
    start, end, products, scope_type, scope_identifier = args
    key = cache_key('agg', [kind, start.isoformat(), end.isoformat(), list(products), scope_type, scope_identifier, data_version()])
    return single_flight(key, lambda: compute(*args))


@api_view(['GET'])
def daily_aggregation(request):
    args, error = _aggregation_request(request)
    if error:
        return error
    rows = _cached_rows('daily', args, daily_aggregates)
    paginator = AggregationPagination()
    page = paginator.paginate_queryset(rows, request)
    return paginator.get_paginated_response(page)


@api_view(['GET'])
def monthly_aggregation(request):
    args, error = _aggregation_request(request, monthly=True)
    if error:
        return error
    rows = _cached_rows('monthly', args, monthly_aggregates)
    paginator = AggregationPagination()
    page = paginator.paginate_queryset(rows, request)
    return paginator.get_paginated_response(page)


@api_view(['GET'])
def harmonized_monthly(request):
    try:
        start_year = int(request.query_params['start_year']) if request.query_params.get('start_year') else None
        end_year = int(request.query_params['end_year']) if request.query_params.get('end_year') else None
    except ValueError:
        raise ValidationError({'detail': 'start_year and end_year must be integers.'})
    return _harmonized_response(request, lambda series: harmonized.monthly_index(series, start_year, end_year))


@api_view(['GET'])
def harmonized_daily(request):
    start = parse_date(request.query_params.get('start_date') or '')
    end = parse_date(request.query_params.get('end_date') or '')
    if start is None or end is None or start > end or (end - start).days > 366:
        raise ValidationError({'detail': 'start_date and end_date are required, in order, and at most 366 days apart.'})
    return _harmonized_response(request, lambda series: harmonized.daily_index(series, start, end))


@api_view(['GET', 'POST'])
def harmonized_archive(request):
    """Status of, or request for, a country's multi-year harmonized archive."""
    code = str(request.data.get('country_code') if request.method == 'POST' else request.query_params.get('country_code') or '').upper()
    if not code:
        raise ValidationError({'detail': 'country_code is required.'})
    if request.method == 'GET':
        return Response({'country_code': code, **archive_prepare.status(code)})
    try:
        match = next((c for c in list_countries()['results'] if c['country_code'] == code), None)
    except (ValueError, RuntimeError) as exc:
        return Response({'detail': str(exc)}, status=503)
    if match is None:
        raise ValidationError({'detail': f'Unknown country code {code}.'})
    return Response({'country_code': code, **archive_prepare.start(code, match['country'])}, status=202)


def _harmonized_response(request, compute):
    bounds, polygon = _resolve_geo_scope(request)
    area = harmonized.resolve_area(bounds, polygon)
    coverage = {key: area[key] for key in ('status', 'coverage_fraction', 'message')}
    coverage['cells'] = len(area['cells'])
    if area['status'] == 'unavailable':
        return Response({'coverage': coverage, 'metadata': None, 'rows': []})
    series = harmonized.load_series(area['cells'])
    return Response({'coverage': coverage, 'metadata': harmonized.metadata(series), 'rows': compute(series)})


@api_view(['GET'])
def coverage_records(request):
    """Return bounded per-attempt coverage evidence, keeping outcomes distinct."""
    queryset = CoverageLedger.objects.all()
    source_product = request.query_params.get('source_product')
    if source_product:
        queryset = queryset.filter(source_product__iexact=source_product)
    start_raw = request.query_params.get('start_date')
    end_raw = request.query_params.get('end_date')
    start = parse_date(start_raw) if start_raw else None
    end = parse_date(end_raw) if end_raw else None
    if start_raw and not start or end_raw and not end:
        return Response({'detail': 'start_date and end_date must use YYYY-MM-DD.'}, status=400)
    if start and end and start > end:
        return Response({'detail': 'start_date must be on or before end_date.'}, status=400)
    if start:
        queryset = queryset.filter(acquisition_date__gte=start)
    if end:
        queryset = queryset.filter(acquisition_date__lte=end)
    scope = request.query_params.get('scope')
    if scope:
        if scope in dict(CoverageLedger.SCOPE_CHOICES):
            queryset = queryset.filter(scope_type=scope)
        else:
            queryset = queryset.filter(scope_identifier=scope)
    queryset = queryset.order_by('-acquisition_date', 'source_product', '-created_at')
    paginator = CoveragePagination()
    page = paginator.paginate_queryset(queryset, request)
    api_key = os.environ.get('FIRMS_MAP_KEY', '')
    def safe_evidence(value):
        if isinstance(value, dict):
            return {str(key): safe_evidence(item) for key, item in value.items()}
        if isinstance(value, list):
            return [safe_evidence(item) for item in value]
        if isinstance(value, str):
            return safe_coverage_error(value, api_key)
        return value

    results = [{
        'source_product': row.source_product,
        'acquisition_date': row.acquisition_date.isoformat(),
        'scope_type': row.scope_type,
        'scope_identifier': row.scope_identifier,
        'availability_outcome': row.availability_outcome,
        'retrieval_outcome': row.retrieval_outcome,
        'observation_outcome': row.observation_outcome,
        'detection_count': row.detection_count,
        'rejected_count': row.rejected_count,
        'retrieved_at': row.retrieved_at.isoformat() if row.retrieved_at else None,
        'live_run_id': row.live_run_id,
        'historical_job_id': str(row.historical_job.job_id) if row.historical_job_id else None,
        'attempt_key': row.attempt_key,
        'evidence': safe_evidence(row.evidence),
    } for row in page]
    return paginator.get_paginated_response(results)


@api_view(['GET'])
def summary(request):
    # Global live summaries group ~2M rows; cache per filter set and data version.
    params = sorted((k, v) for k, v in request.query_params.lists())
    key = cache_key('summary', [params, data_version()])
    return Response(single_flight(key, lambda: _summary_payload(request)))


def _covering_index_rows(queryset):
    """Run a grouped detection query on SQLite through fd_dashboard_cover_idx.

    For date-range scans SQLite prefers the sensor index and then reads table
    pages for every row (~14s over 3M rows); the covering index already holds
    every filtered/grouped column and is ~3x faster. Dates come back as text.
    """
    if connection.vendor != 'sqlite':
        return list(queryset)
    sql, params = queryset.query.sql_with_params()
    table = connection.ops.quote_name(FireDetection._meta.db_table)
    sql = sql.replace(f'FROM {table}', f'FROM {table} INDEXED BY fd_dashboard_cover_idx', 1)
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _summary_payload(request):
    queryset = _apply_filters(FireDetection.objects.all(), request, ('acq_date', 'sensor', 'id'))
    # One grouped scan feeds every section; four separate GROUP BYs over the
    # live table each re-read ~2M rows.
    groups = queryset.order_by().values('acq_date', 'sensor', 'confidence_category').annotate(
        n=Count('id'),
        day=Count('id', filter=Q(daynight='D')),
        night=Count('id', filter=Q(daynight='N')),
        frp_n=Count('frp'),
        frp_sum=Sum('frp'),
    )
    sensor_counts, confidence_counts, by_day = {}, {}, {}
    for row in _covering_index_rows(groups):
        sensor_counts[row['sensor']] = sensor_counts.get(row['sensor'], 0) + row['n']
        category = row['confidence_category']
        confidence_counts[category] = confidence_counts.get(category, 0) + row['n']
        bucket = by_day.setdefault((row['acq_date'], row['sensor']), [0, 0, 0, 0, None])
        bucket[0] += row['n']
        bucket[1] += row['day']
        bucket[2] += row['night']
        bucket[3] += row['frp_n']
        if row['frp_sum'] is not None:
            bucket[4] = (bucket[4] or 0) + row['frp_sum']

    sensors = [{'sensor': k, 'count': v} for k, v in sorted(sensor_counts.items(), key=lambda kv: (-kv[1], kv[0]))]
    confidence = [
        {'confidence_category': k, 'count': v}
        for k, v in sorted(confidence_counts.items(), key=lambda kv: (-kv[1], kv[0] is not None, kv[0] or ''))
    ]
    ordered_days = sorted(by_day.items(), key=lambda kv: kv[0])
    daily_activity = [
        {'acq_date': d, 'sensor': sensor, 'detection_count': n, 'daytime_count': day, 'nighttime_count': night}
        for (d, sensor), (n, day, night, _, _) in ordered_days
    ]
    frp = [
        {'acq_date': d, 'sensor': sensor, 'frp_total': total, 'frp_average': (total / frp_n) if frp_n else None}
        for (d, sensor), (_, _, _, frp_n, total) in ordered_days
    ]

    return {
        'generated_at': timezone.now().isoformat(),
        'processed_files': {
            'sensors': len(sensors),
            'daily_activity': len(daily_activity),
            'frp': len(frp),
            'confidence': len(confidence),
        },
        'sensors': sensors,
        'daily_activity': daily_activity,
        'frp': frp,
        'confidence': confidence,
    }


def _prefer_newest_first_index(queryset):
    """Keep SQLite walking fd_newest_first_idx for paged listings.

    With a live/product filter SQLite otherwise picks the source_product index
    and sorts every matching row (~2M for global live) to return 25. A unary
    '+' on the column disables that index without changing the filter.
    """
    if connection.vendor != 'sqlite':
        return queryset
    where = queryset.query.where
    product_filters = [
        child for child in where.children
        if getattr(getattr(child, 'lhs', None), 'target', None) is not None
        and child.lhs.target.name == 'source_product' and child.lookup_name in ('in', 'iexact')
    ]
    if not product_filters:
        return queryset
    for child in product_filters:
        child.lhs = Func(child.lhs, template='+%(expressions)s', output_field=child.lhs.output_field)
    return queryset


@api_view(['GET'])
def detections(request):
    queryset = _apply_filters(FireDetection.objects.all(), request, ('-acq_date', '-acq_time', '-id'))
    queryset = _prefer_newest_first_index(queryset)
    paginator = PageNumberPagination()
    paginator.page_size = 25
    paginator.page_size_query_param = 'page_size'
    paginator.max_page_size = 200
    page = paginator.paginate_queryset(queryset.values(
        'id',
        'source_product',
        'sensor',
        'latitude',
        'longitude',
        'acq_date',
        'acq_time',
        'brightness',
        'frp',
        'scan',
        'track',
        'confidence_raw',
        'confidence_category',
        'daynight',
        'satellite',
        'instrument',
        'version',
        'record_key',
        'retrieved_at',
    ), request)
    rows = [
        _serialize_detection_row(row)
        for row in page
    ]

    return paginator.get_paginated_response(rows)


@api_view(['GET'])
def map_points(request):
    """Return zoom-aware clusters or bounded real detections for a map viewport."""
    try:
        south = float(request.query_params['south'])
        west = float(request.query_params['west'])
        north = float(request.query_params['north'])
        east = float(request.query_params['east'])
    except (KeyError, TypeError, ValueError):
        return Response({'detail': 'south, west, north and east viewport bounds are required.'}, status=400)
    if not (-90 <= south < north <= 90 and -180 <= west < east <= 180):
        return Response({'detail': 'Invalid viewport bounds.'}, status=400)
    geo_scope = request.query_params.get('geo_scope', 'global')
    geo_identifier = request.query_params.get('geo_scope_identifier', 'world')
    polygon = None
    if geo_scope == 'bbox':
        try:
            _, normalized_identifier = resolve_scope_filter('bbox', geo_identifier)
            requested = json.loads(normalized_identifier)
            south, west = max(south, requested['south']), max(west, requested['west'])
            north, east = min(north, requested['north']), min(east, requested['east'])
        except (ValueError, TypeError, KeyError):
            return Response({'detail': 'Invalid geographic bounding-box scope.'}, status=400)
        if south > north or west > east:
            return Response({'count': 0, 'results': [], 'truncated': False})
    elif geo_scope == 'region':
        parts = geo_identifier.split(':', 3)
        if len(parts) != 4:
            return Response({'detail': 'Region scope identifier must include country, level, boundary ID and name.'}, status=400)
        country_code, region_type, boundary_id, region_name = parts
        try:
            details = get_region_details(country_code, region_type, region_name)
        except (ValueError, RuntimeError) as exc:
            return Response({'detail': str(exc)}, status=400)
        if boundary_id and details.get('boundary_code') != boundary_id:
            return Response({'detail': 'Region boundary identifier no longer matches the selected boundary.'}, status=400)
        polygon = details.get('polygon')
        boundary = details['bbox']
        south, west = max(south, boundary['south']), max(west, boundary['west'])
        north, east = min(north, boundary['north']), min(east, boundary['east'])
        if south > north or west > east:
            return Response({'count': 0, 'results': [], 'truncated': False})
    elif geo_scope != 'global':
        return Response({'detail': 'geo_scope must be global, bbox, or region.'}, status=400)
    queryset = _apply_filters(FireDetection.objects.all(), request, geo=False)
    queryset = queryset.filter(latitude__gte=south, latitude__lte=north,
                               longitude__gte=west, longitude__lte=east)
    try:
        limit = min(1500, max(100, int(request.query_params.get('limit', 1000))))
    except (TypeError, ValueError):
        limit = 1000
    try:
        zoom = min(18, max(0, int(request.query_params.get('zoom', 2))))
    except (TypeError, ValueError):
        zoom = 2
    # Cluster broad views in SQL so the browser receives a bounded representation
    # of every matching detection rather than an arbitrary first page.
    cluster_mode = zoom < 8 or (polygon is not None and zoom < 12)
    # Broad views always cluster, so their total comes from the clusters and a
    # separate full COUNT over millions of rows is skipped.
    total = queryset.count() if polygon is None and not cluster_mode else None
    # Dense close-up views also switch to a progressively finer grid instead of
    # dropping records at the individual-point response cap.
    if total is not None and total > limit:
        cluster_mode = True
    if cluster_mode:
        # Grid width in degrees shrinks with zoom; clamp it to avoid excessive
        # groups at world scale and to keep cluster markers legible.
        cell_size = max(0.02, 180 / (2 ** (zoom + 1)))
        if polygon:
            # Region geometry is applied to every candidate before grouping.
            candidates = queryset.order_by().values(
                'id', 'source_product', 'sensor', 'latitude', 'longitude'
            ).iterator(chunk_size=5000)
            buckets = {}
            for row in candidates:
                if not point_in_geometry(row['longitude'], row['latitude'], polygon):
                    continue
                key = (row['source_product'], row['sensor'],
                       int((row['latitude'] + 90) // cell_size),
                       int((row['longitude'] + 180) // cell_size))
                bucket = buckets.setdefault(key, [0, 0.0, 0.0])
                bucket[0] += 1; bucket[1] += row['latitude']; bucket[2] += row['longitude']
            clusters = [
                {'source_product': key[0], 'sensor': key[1], 'count': value[0],
                 'latitude': value[1] / value[0], 'longitude': value[2] / value[0]}
                for key, value in buckets.items()
            ]
            total = sum(cluster['count'] for cluster in clusters)
        else:
            grid_lat = Floor((F('latitude') + Value(90.0)) / Value(cell_size))
            grid_lon = Floor((F('longitude') + Value(180.0)) / Value(cell_size))
            # Clustering ~1M rows takes seconds; reuse it until data changes.
            sql, sql_params = queryset.order_by().values('id').query.sql_with_params()
            cache_key = 'clusters:' + hashlib.sha256(json.dumps(
                [sql, [str(value) for value in sql_params], cell_size, data_version()],
            ).encode()).hexdigest()
            clusters = single_flight(cache_key, lambda: _covering_index_rows(queryset.order_by().annotate(
                _grid_lat=grid_lat, _grid_lon=grid_lon
            ).values('source_product', 'sensor', '_grid_lat', '_grid_lon').annotate(
                count=Count('id'), latitude=Avg('latitude'), longitude=Avg('longitude')
            ).order_by('source_product', 'sensor', '_grid_lat', '_grid_lon')))
            total = total if total is not None else sum(cluster['count'] for cluster in clusters)
        return Response({'mode': 'clusters', 'count': total, 'results': [],
                         'clusters': clusters, 'truncated': False, 'zoom': zoom})

    candidates = list(queryset.order_by('-acq_date', '-acq_time', '-id').values(
        'id', 'source_product', 'sensor', 'latitude', 'longitude', 'acq_date', 'acq_time',
        'brightness', 'frp', 'confidence_category', 'daynight', 'retrieved_at',
    )[:10000 if polygon else limit])
    rows = [row for row in candidates if point_in_geometry(row['longitude'], row['latitude'], polygon)][:limit] if polygon else candidates
    total = queryset.count() if polygon is None else sum(
        1 for row in queryset.order_by().values('latitude', 'longitude').iterator(chunk_size=5000)
        if point_in_geometry(row['longitude'], row['latitude'], polygon)
    )
    return Response({'mode': 'points', 'count': total, 'results': rows, 'clusters': [],
                     'truncated': total > len(rows)})


@api_view(['GET'])
def daily_activity(request):
    queryset = _apply_filters(FireDetection.objects.all(), request)
    rows = list(
        queryset.values('acq_date', 'sensor')
        .annotate(
            detection_count=Count('id'),
            daytime_count=Count('id', filter=Q(daynight='D')),
            nighttime_count=Count('id', filter=Q(daynight='N')),
        )
        .order_by('acq_date', 'sensor')
    )
    return Response(rows)


@api_view(['GET'])
def frp(request):
    queryset = _apply_filters(FireDetection.objects.all(), request)
    rows = list(
        queryset.values('acq_date', 'sensor')
        .annotate(
            frp_total=Sum('frp'),
            frp_average=Avg('frp'),
        )
        .order_by('-acq_date', 'sensor')
    )
    return Response(rows)


@api_view(['GET'])
def confidence(request):
    queryset = _apply_filters(FireDetection.objects.all(), request)
    rows = list(
        queryset.values('confidence_category')
        .annotate(count=Count('id'))
        .order_by('-count', 'confidence_category')
    )
    return Response(rows)


@api_view(['GET'])
def sensors(request):
    queryset = _apply_filters(FireDetection.objects.all(), request)
    rows = list(
        queryset.values('sensor')
        .annotate(count=Count('id'))
        .order_by('-count', 'sensor')
    )
    return Response(rows)


@api_view(['GET'])
def charts(request):
    if not CHARTS_DIR.exists():
        return Response({'results': []})

    charts = []
    for path in sorted(CHARTS_DIR.glob('*')):
        if path.is_file() and path.suffix.lower() in {'.png', '.jpg', '.jpeg', '.svg', '.webp'}:
            charts.append({
                'name': path.name,
                'path': str(path.relative_to(PROCESSED_DIR)),
                'url': f'/api/charts/?name={path.name}',
                'image_url': f'/api/chart-images/{path.name}/',
            })

    if request.query_params.get('name'):
        selected = [item for item in charts if item['name'] == request.query_params.get('name')]
        return Response({'results': selected})

    return Response({'count': len(charts), 'results': charts})


@api_view(['GET', 'POST'])
def historical_jobs(request):
    if request.method == 'POST':
        try:
            job = HistoricalJobService.create_or_get_job(request.data)
        except ValueError as exc:
            return Response({'detail': str(exc)}, status=400)

        payload = job.as_api_dict()
        return Response({
            'job_id': str(job.job_id),
            'status': job.status,
            'stage': job.stage,
            'progress': job.progress,
            'started_at': payload.get('started_at'),
            'finished_at': payload.get('finished_at'),
            'result': job.result_payload,
        }, status=202)

    jobs = HistoricalJob.objects.order_by('-started_at')[:25]
    return Response({
        'count': jobs.count(),
        'results': [job.as_api_dict() for job in jobs],
    })


@api_view(['GET'])
def historical_job_detail(request, job_id):
    job = get_object_or_404(HistoricalJob, job_id=job_id)
    return Response(job.as_api_dict())


@api_view(['POST'])
def historical_job_retry(request, job_id):
    job = get_object_or_404(HistoricalJob, job_id=job_id)
    if job.status not in {'partial', 'failed', 'interrupted'}:
        return Response({'detail': 'Only partial, failed, or interrupted jobs can be retried.'}, status=400)
    try:
        jobs = HistoricalJobService.retry_job(job)
    except ValueError as exc:
        return Response({'detail': str(exc)}, status=400)
    return Response({
        'retried_job_ids': [str(item.job_id) for item in jobs],
        'count': len(jobs),
        'status': 'queued' if jobs else 'nothing_to_retry',
    }, status=202)


@api_view(['GET'])
def historical_job_coverage(request, job_id):
    job = get_object_or_404(HistoricalJob, job_id=job_id)
    result = job.result_payload or {}
    return Response({
        'job_id': str(job.job_id),
        'requested_dates': result.get('requested_dates', []),
        'covered_dates': result.get('actual_covered_dates', job.actual_covered_dates),
        'missing_dates': result.get('missing_dates', job.missing_dates),
        'products': result.get('datasets', []),
        'failures': result.get('failures', []),
    })


@api_view(['GET'])
def live_data_status(request):
    return Response(LiveRetrievalService.status())


@api_view(['POST'])
@throttle_classes([ScopedRateThrottle])
def live_data_refresh(request):
    products = request.data.get('products')
    if products is not None and not isinstance(products, list):
        return Response({'detail': 'products must be a list of NRT product IDs.'}, status=400)
    try:
        runs = LiveRetrievalService.enqueue(
            products=products,
            window_days=request.data.get('window_days', 5),
        )
    except ValueError as exc:
        return Response({'detail': str(exc)}, status=400)
    return Response({
        'status': 'queued',
        'runs': [{
            'run_id': run.pk,
            'product': run.product,
            'sensor': run.sensor,
            'status': run.status,
            'requested_at': run.requested_at.isoformat(),
            'active_window': [run.window_start.isoformat(), run.window_end.isoformat()],
        } for run in runs],
    }, status=202)


live_data_refresh.cls.throttle_scope = 'live_refresh'


@api_view(['GET'])
def live_data_observations(request):
    queryset = _apply_filters(
        FireDetection.objects.filter(
            source_product__in=LIVE_PRODUCTS.keys(),
            retrieved_at__isnull=False,
        ),
        request,
        ('-acq_date', '-acq_time', '-id'),
    )
    paginator = PageNumberPagination()
    paginator.page_size = 25
    paginator.page_size_query_param = 'page_size'
    paginator.max_page_size = 200
    page = paginator.paginate_queryset(queryset.values(
        'id', 'source_product', 'sensor', 'latitude', 'longitude', 'acq_date', 'acq_time',
        'brightness', 'frp', 'scan', 'track', 'confidence_raw', 'confidence_category',
        'daynight', 'satellite', 'instrument', 'version', 'record_key', 'retrieved_at',
    ), request)
    return paginator.get_paginated_response([_serialize_detection_row(row) for row in page])


@api_view(['GET'])
def historical_job_analytics(request, job_id):
    job = get_object_or_404(HistoricalJob, job_id=job_id)
    detections = HistoricalJobDetection.objects.filter(job=job).values(
        'detection__acq_date',
        'detection__sensor',
        'detection__source_product',
        'detection__frp',
        'detection__daynight',
    )
    daily = {}
    grouped = {}
    frp_daily = {}
    day_night = {}
    covered_dates = set()
    for row in detections.iterator(chunk_size=1000):
        day = row['detection__acq_date'].isoformat()
        sensor = row['detection__sensor']
        product = row['detection__source_product']
        covered_dates.add(day)
        daily_key = (day, sensor)
        daily[daily_key] = daily.get(daily_key, 0) + 1
        group_key = (sensor, product)
        grouped[group_key] = grouped.get(group_key, 0) + 1
        frp_value = row['detection__frp']
        if frp_value is not None:
            frp_key = (day, sensor)
            total, count = frp_daily.get(frp_key, (0.0, 0))
            frp_daily[frp_key] = (total + frp_value, count + 1)
        activity = day_night.setdefault(sensor, {'daytime': 0, 'nighttime': 0, 'unknown': 0})
        if row['detection__daynight'] == 'D':
            activity['daytime'] += 1
        elif row['detection__daynight'] == 'N':
            activity['nighttime'] += 1
        else:
            activity['unknown'] += 1

    coverage = {
        'requested_dates': job.result_payload.get('requested_dates', []),
        'covered_dates': job.actual_covered_dates,
        'missing_dates': job.missing_dates,
        'coverage_ratio': (
            len(job.actual_covered_dates) / len(job.result_payload.get('requested_dates', []))
            if job.result_payload.get('requested_dates') else 0
        ),
    }
    return Response({
        'job_id': str(job.job_id),
        'status': job.status,
        'region': job.request_params.get('region'),
        'date_range': {
            'start_date': job.start_date.isoformat() if job.start_date else None,
            'end_date': job.end_date.isoformat() if job.end_date else None,
        },
        'coverage': coverage,
        'regional_totals': {
            'detections': sum(grouped.values()),
            'covered_dates': len(covered_dates),
            'region': job.request_params.get('region'),
            'bbox': job.request_params.get('bbox'),
        },
        'daily_detections': [
            {'date': day, 'sensor': sensor, 'detection_count': count}
            for (day, sensor), count in sorted(daily.items())
        ],
        'detections_by_sensor_product': [
            {'sensor': sensor, 'source_product': product, 'detection_count': count}
            for (sensor, product), count in sorted(grouped.items())
        ],
        'daily_frp': [
            {
                'date': day,
                'sensor': sensor,
                'frp_total': total,
                'frp_average': total / count,
                'valid_records': count,
            }
            for (day, sensor), (total, count) in sorted(frp_daily.items())
        ],
        'day_night': [
            {'sensor': sensor, **counts}
            for sensor, counts in sorted(day_night.items())
        ],
    })


@api_view(['GET'])
def historical_job_hotspots(request, job_id):
    job = get_object_or_404(HistoricalJob, job_id=job_id)
    try:
        limit = int(request.query_params.get('limit', 200))
        offset = int(request.query_params.get('offset', 0))
    except ValueError:
        return Response({'detail': 'limit and offset must be integers.'}, status=400)
    if not 1 <= limit <= 500 or offset < 0:
        return Response({'detail': 'limit must be 1-500 and offset must be non-negative.'}, status=400)

    queryset = (
        HistoricalJobDetection.objects.filter(
            job=job,
            detection__latitude__gte=-90,
            detection__latitude__lte=90,
            detection__longitude__gte=-180,
            detection__longitude__lte=180,
        )
        .select_related('detection')
        .order_by('detection_id')
    )
    total = queryset.count()
    rows = queryset[offset:offset + limit]
    return Response({
        'job_id': str(job.job_id),
        'count': total,
        'limit': limit,
        'offset': offset,
        'next_offset': offset + limit if offset + limit < total else None,
        'results': [
            {
                'id': item.detection_id,
                'latitude': item.detection.latitude,
                'longitude': item.detection.longitude,
                'acq_date': item.detection.acq_date.isoformat(),
                'acq_time': item.detection.acq_time,
                'sensor': item.detection.sensor,
                'source_product': item.detection.source_product,
                'frp': item.detection.frp,
                'confidence_category': item.detection.confidence_category,
                'daynight': item.detection.daynight,
            }
            for item in rows
        ],
    })


@api_view(['GET'])
def chart_image(request, name):
    allowed_suffixes = {'.png', '.jpg', '.jpeg', '.svg', '.webp'}
    path = CHARTS_DIR / name
    if path.suffix.lower() not in allowed_suffixes or not path.is_file() or path.parent != CHARTS_DIR:
        return Response({'detail': 'Chart image not found.'}, status=404)
    content_type = {
        '.png': 'image/png',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.svg': 'image/svg+xml',
        '.webp': 'image/webp',
    }[path.suffix.lower()]
    return FileResponse(path.open('rb'), content_type=content_type)


@api_view(['GET'])
def administrative_regions(request):
    try:
        page = max(1, int(request.query_params.get('page', 1)))
        page_size = max(1, min(500, int(request.query_params.get('page_size', 100))))
        result = list_regions(
            request.query_params.get('country', ''),
            request.query_params.get('type', request.query_params.get('administrative_level', '')),
            page=page,
            page_size=page_size,
        )
    except (ValueError, RuntimeError) as exc:
        status = 400 if isinstance(exc, ValueError) else 503
        return Response({'detail': str(exc)}, status=status)
    return Response(result)


@api_view(['GET'])
def countries(request):
    try:
        result = list_countries()
        page = max(1, int(request.query_params.get('page', 1)))
        page_size = max(1, min(500, int(request.query_params.get('page_size', 100))))
        results = result['results']
        start = (page - 1) * page_size
        result.update({'count': len(results), 'page': page, 'page_size': page_size, 'results': results[start:start + page_size]})
        return Response(result)
    except (ValueError, RuntimeError) as exc:
        return Response({'detail': str(exc)}, status=503)


@api_view(['GET'])
def administrative_levels(request):
    try:
        result = list_administrative_levels(request.query_params.get('country', ''))
        return Response(result)
    except ValueError as exc:
        return Response({'detail': str(exc)}, status=400)
    except RuntimeError as exc:
        return Response({'detail': str(exc)}, status=503)


@api_view(['GET'])
def region_detail(request):
    try:
        result = get_region_details(
            request.query_params.get('country', ''),
            request.query_params.get('type', request.query_params.get('administrative_level', '')),
            request.query_params.get('name', ''),
        )
        return Response(result)
    except ValueError as exc:
        return Response({'detail': str(exc)}, status=400)
    except RuntimeError as exc:
        return Response({'detail': str(exc)}, status=503)

from __future__ import annotations

import json
import math
import os
import re
import threading
import unicodedata
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

import requests
from django.conf import settings

BOUNDARY_VERSION = 'geoBoundaries gbOpen 5.0.0 (9469f09592ced973a3448cf66b6100b741b64c0d)'
BOUNDARY_SOURCE = 'geoBoundaries gbOpen'
BOUNDARY_LICENSE = 'Creative Commons Attribution 4.0 International (CC BY 4.0)'
BOUNDARY_YEAR = 'release-dependent'
BOUNDARY_PATH = Path(settings.BASE_DIR) / 'fires' / 'data' / 'boundaries' / 'nigeria_adm1_2022.geojson'
DEFAULT_API_URL = 'https://www.geoboundaries.org/api/current/gbOpen'

_download_lock = threading.Lock()
_feature_cache: dict[tuple[str, str], tuple[dict[str, Any], ...]] = {}
_metadata_cache: dict[tuple[str, str], dict[str, Any] | None] = {}


def _setting(name: str, default: Any) -> Any:
    return getattr(settings, name, os.getenv(name, default))


def get_cache_dir() -> Path:
    configured = _setting('GEOBOUNDARIES_CACHE_DIR', Path(settings.BASE_DIR) / 'fires' / 'data' / 'boundaries' / 'cache')
    return Path(configured)


def _api_url() -> str:
    return str(_setting('GEOBOUNDARIES_API_URL', DEFAULT_API_URL)).rstrip('/')


def _normalized_name(value: Any) -> str:
    text = unicodedata.normalize('NFKD', str(value or '')).encode('ascii', 'ignore').decode('ascii')
    text = text.casefold().replace('&', ' and ')
    text = re.sub(r'[^a-z0-9]+', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()


def _name_aliases(value: Any) -> set[str]:
    base = _normalized_name(value)
    aliases = {base}
    aliases.add(re.sub(r'^(state|province|county|district|region|prefecture|department|governorate|territory)\s+', '', base))
    aliases.add(re.sub(r'\s+(state|province|county|district|region|prefecture|department|governorate|territory)$', '', base))
    return {alias for alias in aliases if alias}


def _pycountry():
    try:
        import pycountry
        return pycountry
    except ImportError:
        return None


def resolve_country(value: Any) -> dict[str, str] | None:
    raw = str(value or '').strip()
    if not raw:
        return None
    normalized = _normalized_name(raw)
    pc = _pycountry()
    if pc is not None:
        candidates = list(pc.countries)
        for country in candidates:
            names = [country.name, getattr(country, 'official_name', '')]
            if normalized in {_normalized_name(name) for name in names}:
                return {'name': country.name, 'iso2': country.alpha_2, 'iso3': country.alpha_3}
        upper = raw.upper()
        try:
            country = pc.countries.get(alpha_3=upper)
            if country:
                return {'name': country.name, 'iso2': country.alpha_2, 'iso3': country.alpha_3}
            country = pc.countries.get(alpha_2=upper)
            if country:
                return {'name': country.name, 'iso2': country.alpha_2, 'iso3': country.alpha_3}
        except LookupError:
            pass
    # Preserve common names even when pycountry is unavailable.
    common = {
        'nigeria': ('Nigeria', 'NG', 'NGA'), 'nga': ('Nigeria', 'NG', 'NGA'),
        'united states': ('United States', 'US', 'USA'), 'usa': ('United States', 'US', 'USA'),
        'united states of america': ('United States', 'US', 'USA'),
        'canada': ('Canada', 'CA', 'CAN'), 'can': ('Canada', 'CA', 'CAN'),
        'brazil': ('Brazil', 'BR', 'BRA'), 'bra': ('Brazil', 'BR', 'BRA'),
        'india': ('India', 'IN', 'IND'), 'ind': ('India', 'IN', 'IND'),
        'australia': ('Australia', 'AU', 'AUS'), 'aus': ('Australia', 'AU', 'AUS'),
        'united kingdom': ('United Kingdom', 'GB', 'GBR'), 'uk': ('United Kingdom', 'GB', 'GBR'), 'gbr': ('United Kingdom', 'GB', 'GBR'),
    }
    found = common.get(normalized)
    if found:
        return {'name': found[0], 'iso2': found[1], 'iso3': found[2]}
    return None


def _normalize_adm_level(value: Any) -> str:
    text = _normalized_name(value).replace('-', '_').replace(' ', '_')
    aliases = {
        'country': 'ADM0', 'adm0': 'ADM0',
        'state': 'ADM1', 'states': 'ADM1', 'province': 'ADM1', 'provinces': 'ADM1',
        'adm1': 'ADM1', 'first_level': 'ADM1', 'firstlevel': 'ADM1',
        'county': 'ADM2', 'counties': 'ADM2', 'district': 'ADM2', 'districts': 'ADM2',
        'adm2': 'ADM2', 'second_level': 'ADM2',
    }
    for number in range(0, 6):
        aliases[f'adm{number}'] = f'ADM{number}'
    result = aliases.get(text)
    if result:
        return result
    match = re.fullmatch(r'adm([0-9])', text)
    if match:
        return f'ADM{match.group(1)}'
    return ''


def canonical_region_type(level: str) -> str:
    return {
        'ADM0': 'country', 'ADM1': 'state', 'ADM2': 'county',
        'ADM3': 'district', 'ADM4': 'administrative_unit', 'ADM5': 'administrative_unit',
    }.get(level, level.lower())


def _coordinates(geometry: dict[str, Any]) -> Iterable[list[float]]:
    def walk(value: Any):
        if isinstance(value, list) and len(value) >= 2 and isinstance(value[0], (int, float)) and isinstance(value[1], (int, float)):
            yield value
        elif isinstance(value, list):
            for child in value:
                yield from walk(child)
    yield from walk(geometry.get('coordinates', []))


def _bbox(geometry: dict[str, Any]) -> dict[str, float]:
    points = list(_coordinates(geometry))
    if not points:
        raise ValueError('Boundary geometry has no coordinates.')
    longitudes = [float(p[0]) for p in points]
    latitudes = [float(p[1]) for p in points]
    # A span greater than 180 degrees is treated as a broad antimeridian candidate bbox.
    span = max(longitudes) - min(longitudes)
    if span > 180:
        west, east = -180.0, 180.0
    else:
        west, east = min(longitudes), max(longitudes)
    return {'south': min(latitudes), 'west': west, 'north': max(latitudes), 'east': east}


def _validate_geometry(geometry: dict[str, Any]) -> None:
    geometry_type = geometry.get('type')
    if geometry_type == 'Polygon':
        polygons = [geometry.get('coordinates')]
    elif geometry_type == 'MultiPolygon':
        polygons = geometry.get('coordinates', [])
    else:
        raise ValueError('The matched boundary has an unsupported geometry type.')
    if not polygons:
        raise ValueError('The matched boundary has no polygon geometry.')
    for polygon in polygons:
        if not isinstance(polygon, list) or not polygon:
            raise ValueError('The matched boundary contains an empty polygon.')
        for ring in polygon:
            if not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]:
                raise ValueError('The matched boundary contains an invalid ring.')
            if any(not isinstance(position, list) or len(position) < 2 or not isinstance(position[0], (int, float)) or not isinstance(position[1], (int, float)) or not math.isfinite(position[0]) or not math.isfinite(position[1]) for position in ring):
                raise ValueError('The matched boundary contains invalid coordinates.')


def _feature_metadata(feature: dict[str, Any], country: dict[str, str], level: str, source_metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    props = feature.get('properties', {}) or {}
    source_metadata = source_metadata or {}
    name = props.get('shapeName') or props.get('name') or props.get('NAME') or props.get('Name')
    return {
        'country': country['name'],
        'country_code': country['iso3'],
        'country_iso2': country['iso2'],
        'type': canonical_region_type(level),
        'administrative_level': level,
        'name': name,
        'boundary_id': source_metadata.get('boundaryID') or props.get('shapeID'),
        'boundary_feature_id': props.get('shapeID') or props.get('shapeName'),
        'boundary_code': props.get('shapeISO') or props.get('shapeID'),
        'boundary_source': 'geoBoundaries gbOpen',
        'boundary_version': BOUNDARY_VERSION if source_metadata.get('buildDate') else source_metadata.get('boundaryVersion', BOUNDARY_VERSION),
        'boundary_year': str(source_metadata.get('boundaryYearRepresented') or props.get('shapeGroupYear', '')) or BOUNDARY_YEAR,
        'boundary_license': source_metadata.get('boundaryLicense') or BOUNDARY_LICENSE,
        'bbox': _bbox(feature['geometry']),
    }


def _validate_collection(collection: Any) -> list[dict[str, Any]]:
    if not isinstance(collection, dict) or collection.get('type') != 'FeatureCollection':
        raise RuntimeError('Cached administrative boundary data is not a GeoJSON FeatureCollection.')
    features = collection.get('features')
    if not isinstance(features, list) or not features:
        raise RuntimeError('Administrative boundary data contains no features.')
    return features


@lru_cache(maxsize=1)
def _nigeria_adm1_features() -> tuple[dict[str, Any], ...]:
    with BOUNDARY_PATH.open(encoding='utf-8') as boundary_file:
        collection = json.load(boundary_file)
    features = tuple(feature for feature in _validate_collection(collection) if (feature.get('properties', {}).get('shapeGroup') == 'NGA') and (feature.get('properties', {}).get('shapeType') == 'ADM1'))
    if len(features) != 37:
        raise RuntimeError('Cached Nigeria ADM1 boundary data is incomplete or unexpected.')
    return features


def _metadata_for(country_code: str, level: str, session: requests.Session | None = None) -> dict[str, Any] | None:
    key = (country_code, level)
    if key in _metadata_cache:
        return _metadata_cache[key]
    cache_file = get_cache_dir() / f'{country_code}_{level}_metadata.json'
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    if cache_file.exists():
        try:
            with cache_file.open(encoding='utf-8') as f:
                result = json.load(f)
            if isinstance(result, dict):
                _metadata_cache[key] = result
                return result
        except (OSError, json.JSONDecodeError):
            pass
    try:
        response = (session or requests).get(f'{_api_url()}/{country_code}/{level}/', timeout=30)
        response.raise_for_status()
        result = response.json()
        if not isinstance(result, dict) or not result.get('gjDownloadURL'):
            result = None
    except (requests.RequestException, ValueError):
        result = None
    if result is not None:
        with _download_lock:
            try:
                cache_file.write_text(json.dumps(result), encoding='utf-8')
            except OSError:
                pass
    _metadata_cache[key] = result
    return result


def _download_feature_collection(country_code: str, level: str) -> list[dict[str, Any]]:
    key = (country_code, level)
    if key in _feature_cache:
        return list(_feature_cache[key])
    cache_file = get_cache_dir() / f'{country_code}_{level}.geojson'
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    if cache_file.exists():
        try:
            with cache_file.open(encoding='utf-8') as boundary_file:
                collection = json.load(boundary_file)
            features = _validate_collection(collection)
            _feature_cache[key] = tuple(features)
            return features
        except (OSError, json.JSONDecodeError, RuntimeError):
            try:
                cache_file.unlink(missing_ok=True)
            except OSError:
                pass
    metadata = _metadata_for(country_code, level)
    if not metadata:
        raise RuntimeError(f'Boundary metadata is unavailable for {country_code} {level}. The dataset may not exist or the network may be unavailable.')
    url = metadata.get('gjDownloadURL')
    if not url:
        raise RuntimeError(f'Boundary download URL is unavailable for {country_code} {level}.')
    try:
        with _download_lock:
            if cache_file.exists():
                return _download_feature_collection(country_code, level)
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            collection = response.json()
            features = _validate_collection(collection)
            temporary = cache_file.with_suffix('.geojson.tmp')
            temporary.write_text(json.dumps(collection), encoding='utf-8')
            temporary.replace(cache_file)
    except requests.RequestException as exc:
        raise RuntimeError(f'Could not download boundary dataset for {country_code} {level}: {exc}') from exc
    except (json.JSONDecodeError, ValueError) as exc:
        raise RuntimeError(f'Downloaded boundary dataset for {country_code} {level} is invalid JSON.') from exc
    _feature_cache[key] = tuple(features)
    return features


def resolve_region(region: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    country = resolve_country(region.get('country'))
    if not country:
        raise ValueError(f'Unknown or unsupported country: {region.get("country")}.')
    level = _normalize_adm_level(region.get('type') or region.get('region_type') or region.get('administrative_level'))
    if not level:
        raise ValueError('A valid administrative level or type is required (ADM0, ADM1, ADM2, or a supported name).')
    requested_name = region.get('name') or region.get('region_name')
    if not str(requested_name or '').strip():
        raise ValueError('A region name is required.')

    country_code = country['iso3']
    if country_code == 'NGA' and level == 'ADM1':
        features = _nigeria_adm1_features()
        metadata = {
            'boundaryID': 'NGA-ADM1-27671186',
            'boundaryYearRepresented': '2022',
            'boundaryLicense': BOUNDARY_LICENSE,
            'boundarySource': 'GRID3',
            'buildDate': 'Dec 12, 2023',
        }
    else:
        features = _download_feature_collection(country_code, level)
        metadata = _metadata_for(country_code, level)

    requested_aliases = _name_aliases(requested_name)
    matches = []
    for feature in features:
        props = feature.get('properties', {}) or {}
        values = [props.get('shapeName'), props.get('name'), props.get('NAME'), props.get('Name'), props.get('shapeISO')]
        for value in values:
            if requested_aliases & _name_aliases(value):
                matches.append(feature)
                break
    if not matches:
        if country['iso3'] == 'NGA' and level == 'ADM1':
            raise ValueError(f'Unknown Nigeria state: {requested_name}.')
        raise ValueError(f'Unknown {canonical_region_type(level)} name "{requested_name}" in {country["name"]} {level}.')
    if len(matches) > 1:
        ids = sorted({str(f.get('properties', {}).get('shapeID') or f.get('properties', {}).get('shapeISO') or '') for f in matches})
        raise ValueError(f'Ambiguous {canonical_region_type(level)} name "{requested_name}" in {country["name"]}. Identifying information is required (possible boundary IDs: {", ".join(ids)}).')

    feature = matches[0]
    geometry = dict(feature.get('geometry') or {})
    _validate_geometry(geometry)
    resolved = _feature_metadata(feature, country, level, metadata)
    resolved['bbox'] = _bbox(geometry)
    geometry['_bbox'] = resolved['bbox']
    return resolved, geometry


def list_countries() -> dict[str, Any]:
    """List geoBoundaries-indexed ISO countries with lightweight metadata."""
    cache_file = get_cache_dir() / 'countries.json'
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    if cache_file.exists():
        try:
            result = json.loads(cache_file.read_text(encoding='utf-8'))
            if isinstance(result, list):
                return {'count': len(result), 'boundary_source': BOUNDARY_SOURCE, 'boundary_version': BOUNDARY_VERSION, 'boundary_license': BOUNDARY_LICENSE, 'results': result}
        except (OSError, json.JSONDecodeError):
            pass
    try:
        response = requests.get(_api_url() + '/', timeout=30)
        response.raise_for_status()
        html = response.text
    except requests.RequestException as exc:
        raise RuntimeError(f'Could not retrieve the global country boundary index: {exc}') from exc
    codes = sorted(set(re.findall(r'href="([A-Z]{3})/"', html)))
    if not codes:
        raise RuntimeError('The global country boundary index returned no country datasets.')
    pc = _pycountry()
    results = []
    for code in codes:
        country = pc.countries.get(alpha_3=code) if pc else None
        if country:
            results.append({'country': country.name, 'country_code': code, 'country_iso2': country.alpha_2})
        else:
            resolved = resolve_country(code)
            results.append({'country': resolved['name'] if resolved else code, 'country_code': code, 'country_iso2': resolved.get('iso2', '') if resolved else ''})
    results.sort(key=lambda item: item['country'].casefold())
    try:
        cache_file.write_text(json.dumps(results), encoding='utf-8')
    except OSError:
        pass
    return {'count': len(results), 'boundary_source': BOUNDARY_SOURCE, 'boundary_version': BOUNDARY_VERSION, 'boundary_license': BOUNDARY_LICENSE, 'results': results}


def list_administrative_levels(country: str) -> dict[str, Any]:
    resolved = resolve_country(country)
    if not resolved:
        raise ValueError(f'Unknown or unsupported country: {country}.')
    available = []
    for level in ('ADM0', 'ADM1', 'ADM2', 'ADM3', 'ADM4', 'ADM5'):
        metadata = _metadata_for(resolved['iso3'], level)
        if metadata:
            available.append({'administrative_level': level, 'type': canonical_region_type(level), 'canonical_name': metadata.get('boundaryCanonical'), 'count': metadata.get('admUnitCount')})
    if not available:
        raise RuntimeError(f'No indexed administrative boundary levels are available for {resolved["name"]}.')
    return {'country': resolved['name'], 'country_code': resolved['iso3'], 'country_iso2': resolved['iso2'], 'boundary_source': BOUNDARY_SOURCE, 'boundary_version': BOUNDARY_VERSION, 'boundary_license': BOUNDARY_LICENSE, 'results': available}


def list_regions(country: str, region_type: str, page: int = 1, page_size: int = 100) -> dict[str, Any]:
    resolved = resolve_country(country)
    if not resolved:
        raise ValueError(f'Unknown or unsupported country: {country}.')
    level = _normalize_adm_level(region_type)
    if not level:
        raise ValueError('A valid administrative level or type is required (ADM0, ADM1, ADM2, or ADM0-ADM5).')
    country_code = resolved['iso3']
    if country_code == 'NGA' and level == 'ADM1':
        features = _nigeria_adm1_features()
        metadata = {
            'boundaryID': 'NGA-ADM1-27671186',
            'boundaryYearRepresented': '2022',
            'boundaryLicense': BOUNDARY_LICENSE,
            'boundarySource': 'GRID3',
            'buildDate': 'Dec 12, 2023',
        }
    else:
        features = _download_feature_collection(country_code, level)
        metadata = _metadata_for(country_code, level)

    results = []
    for feature in features:
        props = feature.get('properties', {}) or {}
        name = props.get('shapeName') or props.get('name') or props.get('NAME') or props.get('Name')
        if name:
            results.append(name)
    results.sort(key=lambda item: str(item).casefold())
    page = max(1, page)
    page_size = max(1, min(500, page_size))
    start = (page - 1) * page_size
    paged = results[start:start + page_size]
    return {'country': resolved['name'], 'country_code': resolved['iso3'], 'country_iso2': resolved['iso2'], 'type': canonical_region_type(level), 'administrative_level': level, 'boundary_source': BOUNDARY_SOURCE, 'boundary_version': BOUNDARY_VERSION, 'boundary_year': metadata.get('boundaryYearRepresented', '') if metadata else '', 'boundary_license': metadata.get('boundaryLicense', BOUNDARY_LICENSE) if metadata else BOUNDARY_LICENSE, 'count': len(results), 'page': page, 'page_size': page_size, 'results': paged}


def get_region_details(country: str, region_type: str, name: str) -> dict[str, Any]:
    resolved_region, geometry = resolve_region({'country': country, 'type': region_type, 'name': name})
    return {**resolved_region, 'polygon': geometry}


def _point_on_segment(point: tuple[float, float], start: list[float], end: list[float]) -> bool:
    x, y = point
    x1, y1 = start[0], start[1]
    x2, y2 = end[0], end[1]
    cross = (x - x1) * (y2 - y1) - (y - y1) * (x2 - x1)
    scale = max(1.0, abs(x2 - x1), abs(y2 - y1))
    if not math.isclose(cross, 0.0, abs_tol=1e-10 * scale):
        return False
    return min(x1, x2) - 1e-10 <= x <= max(x1, x2) + 1e-10 and min(y1, y2) - 1e-10 <= y <= max(y1, y2) + 1e-10


def _ring_contains(point: tuple[float, float], ring: list[list[float]]) -> tuple[bool, bool]:
    x, y = point
    inside = False
    for index, start in enumerate(ring):
        end = ring[(index + 1) % len(ring)]
        if _point_on_segment(point, start, end):
            return True, True
        x1, y1 = start[0], start[1]
        x2, y2 = end[0], end[1]
        if (y1 > y) != (y2 > y):
            crossing_x = (x2 - x1) * (y - y1) / (y2 - y1) + x1
            if x < crossing_x:
                inside = not inside
    return inside, False


def _polygon_contains(point: tuple[float, float], polygon: list[list[list[float]]]) -> bool:
    if not polygon:
        return False
    in_outer, on_outer_boundary = _ring_contains(point, polygon[0])
    if on_outer_boundary:
        return True
    if not in_outer:
        return False
    for hole in polygon[1:]:
        in_hole, on_hole_boundary = _ring_contains(point, hole)
        if on_hole_boundary:
            return True
        if in_hole:
            return False
    return True


def point_in_geometry(longitude: float, latitude: float, geometry: dict[str, Any]) -> bool:
    geometry_type = geometry.get('type')
    coordinates = geometry.get('coordinates', [])
    bounds = geometry.get('_bbox') or _bbox(geometry)
    candidate_longitudes = (longitude, longitude - 360, longitude + 360)
    contained = False
    for candidate in candidate_longitudes:
        if bounds['west'] <= candidate <= bounds['east'] or bounds['east'] - bounds['west'] >= 359.9:
            point = (candidate, latitude)
            if geometry_type == 'Polygon':
                contained = _polygon_contains(point, coordinates)
            elif geometry_type == 'MultiPolygon':
                contained = any(_polygon_contains(point, polygon) for polygon in coordinates)
            if contained:
                return True
    return False

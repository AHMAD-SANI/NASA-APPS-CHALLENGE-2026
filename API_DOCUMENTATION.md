# FireScope API Documentation

All routes are served below `/api/`. Endpoints are public and do not currently require authentication.

## `GET /api/health/`

Returns backend readiness and generation time.

## `GET /api/summary/`

Returns aggregate sensor, daily activity, FRP, and confidence data.

## `GET /api/detections/`

Returns paginated detection records.

Query parameters: `sensor`, `source_product`, `daynight`, `confidence_category`, `start_date`, `end_date`, `page`, `page_size` (maximum 200).

The response uses DRF pagination:

```json
{
  "count": 0,
  "next": null,
  "previous": null,
  "results": []
}
```

## `GET /api/daily-activity/`

Query parameters use the same supported filters as detections.

## `GET /api/frp/`

Returns daily FRP totals and averages. Uses the same filters as detections.

## `GET /api/confidence/`

Returns detection counts by confidence category.

## `GET /api/sensors/`

Returns detection counts by sensor.

## `GET /api/charts/`

Lists generated chart images. Optional query: `name`.

## `GET /api/chart-images/<name>/`

Returns a bounded list of supported image formats. Traversal outside the chart directory is rejected.

## Geographic discovery endpoints

- `GET /api/countries/?page=1&page_size=100` lists countries indexed by geoBoundaries.
- `GET /api/administrative-levels/?country=Brazil` lists available ADM levels for a country.
- `GET /api/regions/?country=USA&type=state&page=1&page_size=100` lists lightweight region names for a selected country and level.
- `GET /api/regions/detail/?country=Canada&type=province&name=Ontario` returns resolved polygon metadata and GeoJSON geometry.

Country values accept names and ISO2/ISO3 codes. Administrative levels accept `ADM0` through `ADM5`, plus common aliases such as `country`, `state`, `province`, `county`, and `district` where supplied by the source.

Pinned Nigeria example: `GET /api/regions/?country=NGA&type=state`.

## Historical jobs

### Create: `POST /api/historical-jobs/`

Example global bbox request:

```json
{
  "start_date": "2024-01-01",
  "end_date": "2024-01-02",
  "sensors": ["MODIS"],
  "bbox": {"south": -10, "west": 10, "north": 10, "east": 20}
}
```

Named-region examples:

```json
{"start_date":"2026-01-10","end_date":"2026-01-11","sensors":["MODIS"],"region":{"country":"Nigeria","type":"state","name":"Lagos"}}
```

```json
{"start_date":"2026-01-10","end_date":"2026-01-11","sensors":["MODIS"],"region":{"country":"USA","type":"state","name":"California"}}
```

```json
{"start_date":"2026-01-10","end_date":"2026-01-11","sensors":["MODIS"],"region":{"country":"CAN","type":"province","name":"Ontario"}}
```

```json
{"start_date":"2026-01-10","end_date":"2026-01-11","sensors":["MODIS"],"region":{"country":"Brazil","type":"state","name":"Amazonas"}}
```

Supported sensors: `MODIS`, `MODIS_NRT`, `VIIRS_SNPP`, `VIIRS_SNPP_NRT`, `VIIRS_NOAA20`, `VIIRS_NOAA20_NRT`, `VIIRS_NOAA21`, `VIIRS_NOAA21_NRT`.

Named regions and explicit bboxes are mutually exclusive. Date ranges are limited to five years per request. Successful creation returns `202` with a job ID and status.

### List: `GET /api/historical-jobs/`

Returns the latest 25 jobs in an unpaginated list wrapper.

### Detail: `GET /api/historical-jobs/<uuid>/`

Returns status, progress, dates, area, sensors, products, coverage, result, errors, and timestamps.

### Analytics: `GET /api/historical-jobs/<uuid>/analytics/`

Returns job-scoped coverage, daily detections, sensor/product totals, daily FRP, and day/night totals.

### Hotspots: `GET /api/historical-jobs/<uuid>/hotspots/?limit=200&offset=0`

Returns paginated job-specific detections. `limit` is 1–500 and `offset` is non-negative.

Workflow:

1. POST a job.
2. Poll the detail route until `status` is terminal.
3. Read analytics for aggregated job results.
4. Read hotspots with `limit` and `offset` for records.

`completed` means the requested source coverage completed without recorded failures. `partial` means some requested coverage was retrieved. `failed` means no usable coverage was established.

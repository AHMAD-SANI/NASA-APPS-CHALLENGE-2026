# Offline NASA Level-2 source ingestion

This workflow validates locally acquired source files, parses supported granules, and writes a reviewable staging bundle. It never writes observations to `FireDetection` and never performs network access.

## Supported parser inputs

The current candidate support is limited to:

- MOD14 and MYD14, Collection 6.1 HDF4-EOS swath files.
- VNP14IMG, VJ114IMG and VJ214IMG, Collection 2 VIIRS 375 m Level-2 NetCDF4/HDF5 files.

Each artifact set in the Phase 3E manifest also requires its documented geolocation and radiance companion plus the NASA granule metadata artifact. The parser joins fire pixels to the same-index geolocation pixel using the product's native zero-based line/sample locator. MODIS uses MOD03/MYD03; VIIRS 375 m uses Vxx03IMG. The 750 m Vxx03MODLL geolocation product is not a substitute for the 375 m I-band companion.

The parser retains raw mask/algorithm QA and geolocation quality values. It does not decode undocumented QA bits, invent footprints, infer VIIRS day/night polarity from `FP_day`, or assign a timestamp to each pixel when the source only provides a granule interval. Unsupported product versions fail closed.

## Install dependencies

Use the backend environment and install the project manifest:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip show pyhdf netCDF4
.\.venv\Scripts\python.exe -m pip check
```

`pyhdf` reads HDF4 MODIS files; `netCDF4` reads VIIRS NetCDF4/HDF5 files. h5py, GDAL and rasterio are not required by this workflow. On Windows, install wheels matching the active Python version and architecture. If PyPI access is unavailable, use an organization-approved wheelhouse or package mirror; do not embed Earthdata credentials or tokens in a package URL, manifest, logs or source code.

## Acquire and validate locally

1. Acquire the original files using an authorized NASA archive workflow and authenticated Earthdata access when required.
2. Place each original artifact under the artifact manifest directory. Keep originals unchanged. Use one directory for the five fire products and their matching geolocation, radiance and metadata artifacts.
3. Fill the Phase 3E manifest with actual granule IDs, relationships, product/version metadata, relative paths, sizes and SHA-256 hashes. The starter manifest is intentionally empty; do not fill unknown fields by assumption.
4. Run the existing artifact validator, then stage parsed records:

```powershell
.\.venv\Scripts\python.exe manage.py validate_source_artifacts --manifest path\to\manifest.json
.\.venv\Scripts\python.exe manage.py ingest_source_artifacts --manifest path\to\manifest.json --output-dir path\to\staging\run-001 --preview-rows 100
```

The output contains `observations.csv`, `preview.json` and `ingestion_manifest.json`. Check product identity, collection, granule relationships, dimensions, native pixel locators, coordinates and QA before using the staging output. Re-running the exact same input and parser version against the completed output directory is idempotent; a changed input requires a new output directory. No production database import command is provided in this phase.

## Validation status

Tests use synthetic in-memory datasets and synthetic byte fixtures. They verify parser logic, provenance, checksums, staging and idempotency; they do not establish compatibility with real NASA granules. Before pilot approval, run against at least one acquired granule and its exact companions for each of MOD14, MYD14, VNP14IMG, VJ114IMG and VJ214IMG, and compare parsed metadata and pixel records with NASA documentation or an independent reader.

"""Offline parsers for NASA MOD14/MYD14 and VIIRS 375 m Level-2 artifacts.

This module reads local files only. It emits staging records and never writes to
the FireDetection table. Real-product compatibility must still be checked against
actual acquired NASA granules; unit fixtures exercise schema and validation logic.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from fires.source_artifacts import ARTIFACT_SETS, validate_manifest


PARSER_VERSION = "firescope-l2-parser/0.1.0"
FIRE_CLASSES = {7, 8, 9}
PRODUCT_LAYOUTS = {
    "MOD14": {"platform": "Terra", "instrument": "MODIS", "family": "modis"},
    "MYD14": {"platform": "Aqua", "instrument": "MODIS", "family": "modis"},
    "VNP14IMG": {"platform": "Suomi-NPP", "instrument": "VIIRS", "family": "viirs"},
    "VJ114IMG": {"platform": "NOAA-20", "instrument": "VIIRS", "family": "viirs"},
    "VJ214IMG": {"platform": "NOAA-21", "instrument": "VIIRS", "family": "viirs"},
}

MODIS_FIELDS = (
    "FP_line", "FP_sample", "FP_latitude", "FP_longitude", "FP_T21", "FP_T31",
    "FP_power", "FP_confidence", "FP_AdjCloud", "FP_AdjWater", "FP_WinSize",
    "FP_NumValid", "FP_land", "FP_ViewZenAng", "FP_SolZenAng", "FP_RelAzAng",
)
VIIRS_FIELDS = (
    "FP_line", "FP_sample", "FP_latitude", "FP_longitude", "FP_T4", "FP_T5",
    "FP_power", "FP_confidence", "FP_day", "FP_AdjCloud", "FP_AdjWater",
    "FP_WinSize", "FP_SolZenAng", "FP_SolAzAng", "FP_ViewZenAng", "FP_ViewAzAng",
)
FIELD_LABELS = {
    "FP_T21": ("T21_K", "K"), "FP_T31": ("T31_K", "K"),
    "FP_T4": ("T4_K", "K"), "FP_T5": ("T5_K", "K"),
    "FP_power": ("frp_mw", "MW"),
    "FP_confidence": ("confidence_native", None),
    "FP_AdjCloud": ("adjacent_cloud_count", None),
    "FP_AdjWater": ("adjacent_water_count", None),
    "FP_WinSize": ("background_window_size", None),
    "FP_NumValid": ("valid_background_count", None),
    "FP_land": ("land_flag", None),
    "FP_ViewZenAng": ("view_zenith_deg", "degrees"),
    "FP_SolZenAng": ("solar_zenith_deg", "degrees"),
    "FP_RelAzAng": ("relative_azimuth_deg", "degrees"),
    "FP_SolAzAng": ("solar_azimuth_deg", "degrees"),
    "FP_ViewAzAng": ("view_azimuth_deg", "degrees"),
}


class SourceProductError(ValueError):
    """The source file is incomplete, inconsistent, or outside supported schema."""


@dataclass
class DatasetView:
    """Small adapter shared by HDF4, NetCDF4 and synthetic parser fixtures."""

    shape: tuple[int, ...]
    read_all: object
    read_row: object | None = None
    attributes: dict | None = None


@dataclass
class ProductFile:
    path: Path
    attributes: dict
    datasets: dict[str, DatasetView]


def _text(value):
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace").strip("\x00 \t\r\n")
    if isinstance(value, np.generic):
        return _text(value.item())
    if isinstance(value, np.ndarray) and value.size == 1:
        return _text(value.reshape(-1)[0])
    if isinstance(value, (list, tuple)) and len(value) == 1:
        return _text(value[0])
    return str(value).strip("\x00 \t\r\n") if value is not None else None


def _plain(value):
    if value is None:
        return None
    if isinstance(value, np.ma.MaskedArray):
        if np.ma.is_masked(value):
            return None
        value = value.item() if value.ndim == 0 else value.tolist()
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and (not math.isfinite(value)):
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    return value


def _attr(attrs, *names):
    lower = {str(key).lower(): value for key, value in attrs.items()}
    for name in names:
        value = lower.get(name.lower())
        if value is not None:
            return _text(value)
    return None


def _pvl_values(attrs):
    """Read scalar values from ECS PVL global metadata without losing raw text."""
    result = {}
    for attr_name in ("CoreMetadata.0", "ArchiveMetadata.0"):
        raw = attrs.get(attr_name)
        text = _text(raw)
        if not text:
            continue
        for line in text.splitlines():
            match = re.match(r"\s*([A-Za-z][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$", line)
            if match:
                val = match.group(2).strip().strip('"')
                result.setdefault(match.group(1), val)
    return result


def _merged_attributes(product_file: ProductFile):
    merged = dict(product_file.attributes)
    for key, value in _pvl_values(product_file.attributes).items():
        merged.setdefault(key, value)
    return merged


def _iso_datetime(date_value, time_value):
    if not date_value or not time_value:
        return None
    date_value = _text(date_value)
    time_value = _text(time_value)
    try:
        return datetime.fromisoformat(f"{date_value}T{time_value.replace('Z', '+00:00')}").replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    except ValueError:
        try:
            parsed = datetime.strptime(f"{date_value} {time_value.split('.')[0]}", "%Y-%m-%d %H:%M:%S")
            return parsed.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        except ValueError:
            return None


def _source_metadata(product_file: ProductFile, expected_short_name: str, manifest_artifact: dict):
    attrs = _merged_attributes(product_file)
    internal_short = _attr(attrs, "SHORTNAME", "ShortName")
    if not internal_short:
        doi = _attr(attrs, "identifier_product_doi", "ProductDOI") or ""
        match = re.search(r"(MOD14|MYD14|VNP14IMG|VJ114IMG|VJ214IMG)", doi, re.IGNORECASE)
        internal_short = match.group(1).upper() if match else None
    if internal_short and expected_short_name not in internal_short:
        raise SourceProductError(f"Internal product identifier {internal_short!r} does not match {expected_short_name}.")
    if not internal_short:
        # Filename identity alone is not enough for a scientific import.
        raise SourceProductError("Product-internal short-name metadata is missing.")
    platform = _attr(attrs, "PLATFORM", "PlatformShortName", "platform_short_name")
    instrument = _attr(attrs, "INSTRUMENTNAME", "InstrumentShortName", "instrument_short_name")
    if platform and platform.lower() not in manifest_artifact["platform"].lower() and manifest_artifact["platform"].lower() not in platform.lower():
        raise SourceProductError(f"Internal platform {platform!r} does not match manifest platform.")
    if instrument and instrument.lower() != manifest_artifact["instrument"].lower():
        raise SourceProductError(f"Internal instrument {instrument!r} does not match manifest instrument.")

    filename = product_file.path.name
    token = re.search(r"\.A(\d{4})(\d{3})\.(\d{4})\.(\d{3})\.", filename)
    if not token:
        raise SourceProductError("Filename does not contain the documented acquisition date/time and collection tokens.")
    year, doy, hhmm, filename_collection = token.groups()
    if filename_collection.lstrip("0") != str(manifest_artifact["collection"]).lstrip("0"):
        raise SourceProductError("Filename collection token does not match the manifest collection.")
    parsed_date = datetime.strptime(f"{year}-{doy}", "%Y-%j").date().isoformat()
    if parsed_date != manifest_artifact["acquisition_date_utc"]:
        raise SourceProductError("Filename acquisition date does not match manifest scope.")

    start_date = _attr(attrs, "RANGEBEGINNINGDATE", "RangeBeginningDate", "time_coverage_start")
    start_time = _attr(attrs, "RANGEBEGINNINGTIME", "RangeBeginningTime")
    end_date = _attr(attrs, "RANGEENDINGDATE", "RangeEndingDate")
    end_time = _attr(attrs, "RANGEENDINGTIME", "RangeEndingTime")
    start = _iso_datetime(start_date, start_time)
    end = _iso_datetime(end_date, end_time)
    if start and start_time:
        parsed_start = datetime.fromisoformat(start.replace("Z", "+00:00"))
        if parsed_start.strftime("%H%M") != hhmm:
            raise SourceProductError("Internal range-beginning time does not match the documented filename time token.")
    if start and end and datetime.fromisoformat(end.replace("Z", "+00:00")) < datetime.fromisoformat(start.replace("Z", "+00:00")):
        raise SourceProductError("Internal granule ending time precedes its beginning time.")
    if start is None:
        # The filename carries only an approximate acquisition start; retain that
        # as granule time and make exact start/end unavailable rather than invent it.
        start = datetime.strptime(f"{year}-{doy} {hhmm}", "%Y-%j %H%M").replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    granule_id = _attr(attrs, "LOCALGRANULEID", "LocalGranuleID", "GranuleID")
    if granule_id and Path(granule_id).name != filename:
        raise SourceProductError("Internal granule ID does not match the local original filename.")
    processing_version = _attr(attrs, "LOCALVERSIONID", "ProcessingVersion", "PGEVersion", "VersionID", "AlgorithmVersion")
    return {
        "product_short_name": expected_short_name,
        "platform": manifest_artifact["platform"],
        "instrument": manifest_artifact["instrument"],
        "collection": manifest_artifact["collection"],
        "processing_version": processing_version,
        "manifest_processing_version": manifest_artifact.get("processing_version"),
        "granule_id": granule_id or manifest_artifact["granule_id"],
        "acquisition_start_utc": start,
        "acquisition_end_utc": end,
        "native_attributes": {str(key): _text(value) for key, value in product_file.attributes.items()},
        "internal_short_name": internal_short,
        "internal_platform": platform,
        "internal_instrument": instrument,
    }


def _dataset(product_file: ProductFile, names, required=True):
    found = []
    seen = set()
    for name in names:
        dataset = product_file.datasets.get(name)
        if dataset is not None and id(dataset) not in seen:
            seen.add(id(dataset))
            found.append(dataset)
    if len(found) > 1:
        raise SourceProductError(f"Ambiguous dataset aliases present: {names}.")
    if not found:
        if required:
            raise SourceProductError(f"Required dataset missing; expected one of {names}.")
        return None
    return found[0]


def _valid_scalar(value):
    value = _plain(value)
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _raw_field_value(dataset: DatasetView | None, index: int):
    if dataset is None:
        return None
    values = np.asanyarray(dataset.read_all())
    if values.ndim != 1 or index >= len(values):
        raise SourceProductError("Sparse fire-pixel vectors have inconsistent dimensions.")
    value = _plain(values[index])
    if value is None:
        return None
    attrs = dataset.attributes or {}
    fill = attrs.get("_FillValue", attrs.get("fill_value"))
    if fill is not None and value == _plain(fill):
        return None
    return value


def _reader_fields(product_file, fields):
    result = {}
    for field in fields:
        result[field] = _dataset(product_file, (field,), required=field in {"FP_line", "FP_sample", "FP_latitude", "FP_longitude"})
    return result


def _row(dataset: DatasetView, line: int):
    if dataset.read_row is not None:
        return np.asanyarray(dataset.read_row(line)).reshape(-1)
    return np.asanyarray(dataset.read_all())[line, :].reshape(-1)


def _coordinate_valid(lat, lon):
    lat, lon = _valid_scalar(lat), _valid_scalar(lon)
    if lat is None or lon is None or not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        return None, None
    return lat, lon


def _daynight(attrs, field_value=None):
    value = _attr(attrs, "DayNightFlag", "daynight", "DayNight")
    if value is None:
        return None
    value = value.upper()
    if value in {"D", "DAY"}:
        return "D"
    if value in {"N", "NIGHT"}:
        return "N"
    # C2 documents FP_day as a 0/1 flag but the user guide table does not
    # define its numeric polarity. Preserve the native value separately.
    return None


def _parse_arrays(product_file: ProductFile, artifact: dict, set_id: str, family: str, companion: ProductFile | None = None):
    expected = artifact["product_short_name"]
    if expected not in PRODUCT_LAYOUTS or PRODUCT_LAYOUTS[expected]["family"] != family:
        raise SourceProductError(f"Unsupported product family for {expected}.")
    metadata = _source_metadata(product_file, expected, artifact)
    mask = _dataset(product_file, ("fire mask", "fire_mask"))
    qa = _dataset(product_file, ("algorithm QA", "algorithm_QA", "algorithm_qa"))
    if len(mask.shape) != 2 or len(qa.shape) != 2 or mask.shape != qa.shape:
        raise SourceProductError("Fire mask and algorithm QA must be two-dimensional with identical shapes.")
    fields = _reader_fields(product_file, MODIS_FIELDS if family == "modis" else VIIRS_FIELDS)
    line_values = np.asanyarray(fields["FP_line"].read_all()).reshape(-1)
    sample_values = np.asanyarray(fields["FP_sample"].read_all()).reshape(-1)
    if len(line_values) != len(sample_values):
        raise SourceProductError("FP_line and FP_sample vector lengths differ.")
    count = len(line_values)
    firepix = _attr(_merged_attributes(product_file), "FirePix")
    if firepix is None and count == 0:
        raise SourceProductError("Cannot classify an empty fire-pixel table without the product FirePix count attribute.")
    if firepix is not None:
        try:
            if int(float(firepix)) != count:
                raise SourceProductError("FirePix global attribute disagrees with sparse fire-pixel table length.")
        except ValueError as exc:
            raise SourceProductError("FirePix global attribute is not an integer.") from exc
    for name, dataset in fields.items():
        if dataset is not None:
            values = np.asanyarray(dataset.read_all())
            if values.ndim != 1 or len(values) != count:
                raise SourceProductError(f"{name} vector length does not match FP_line/FP_sample.")

    field_vectors = {
        name: np.asanyarray(dataset.read_all()).reshape(-1)
        for name, dataset in fields.items() if dataset is not None
    }

    def field_value(name, index):
        if name not in field_vectors:
            return None
        value = _plain(field_vectors[name][index])
        attrs = fields[name].attributes or {}
        fill = attrs.get("_FillValue", attrs.get("fill_value"))
        if fill is not None and value == _plain(fill):
            return None
        return value

    geo_lat = geo_lon = None
    geo_quality = None
    if companion is not None:
        lat_names = ("Latitude", "latitude", "geolocation_data/latitude", "geolocation_data/Latitude")
        lon_names = ("Longitude", "longitude", "geolocation_data/longitude", "geolocation_data/Longitude")
        geo_lat, geo_lon = _dataset(companion, lat_names), _dataset(companion, lon_names)
        quality_names = ("gflags",) if family == "modis" else ("quality_flag", "geolocation_data/quality_flag")
        geo_quality = _dataset(companion, quality_names)
        if geo_lat.shape != mask.shape or geo_lon.shape != mask.shape:
            raise SourceProductError("Geolocation companion dimensions do not match the fire-mask line/sample grid.")
        if geo_quality.shape != mask.shape:
            raise SourceProductError("Geolocation quality dimensions do not match the fire-mask line/sample grid.")

    rows_by_line = {}
    geo_rows_by_line = {}
    output = []
    for i, (line_raw, sample_raw) in enumerate(zip(line_values, sample_values)):
        try:
            line_number, sample_number = float(line_raw), float(sample_raw)
            if not math.isfinite(line_number) or not math.isfinite(sample_number) or not line_number.is_integer() or not sample_number.is_integer():
                raise ValueError
            line, sample = int(line_number), int(sample_number)
        except (TypeError, ValueError, OverflowError) as exc:
            raise SourceProductError(f"Sparse pixel locator at row {i} is not an integer index.") from exc
        if line < 0 or sample < 0 or line >= mask.shape[0] or sample >= mask.shape[1]:
            raise SourceProductError(f"Sparse pixel index {(line, sample)} is outside mask dimensions {mask.shape}.")
        if line not in rows_by_line:
            mask_row = _row(mask, line)
            qa_row = _row(qa, line)
            if len(mask_row) != mask.shape[1] or len(qa_row) != qa.shape[1]:
                raise SourceProductError("Row reader returned an inconsistent pixel width.")
            rows_by_line[line] = (mask_row, qa_row)
        mask_row, qa_row = rows_by_line[line]
        fire_class = int(mask_row[sample])
        if fire_class not in FIRE_CLASSES:
            raise SourceProductError(f"Sparse fire table locator {(line, sample)} points to non-fire mask class {fire_class}.")
        qa_value = _plain(qa_row[sample])
        if qa_value is None:
            raise SourceProductError(f"Algorithm QA is missing at sparse fire locator {(line, sample)}.")
        embedded_lat = field_value("FP_latitude", i)
        embedded_lon = field_value("FP_longitude", i)
        lat, lon = _coordinate_valid(embedded_lat, embedded_lon)
        companion_lat = companion_lon = None
        if geo_lat is not None:
            if line not in geo_rows_by_line:
                geo_rows_by_line[line] = (_row(geo_lat, line), _row(geo_lon, line), _row(geo_quality, line))
            geo_lat_row, geo_lon_row, geo_quality_row = geo_rows_by_line[line]
            companion_lat, companion_lon = _coordinate_valid(geo_lat_row[sample], geo_lon_row[sample])
            if companion_lat is None:
                raise SourceProductError(f"Companion geolocation is invalid at sparse fire locator {(line, sample)}.")
            lat, lon = companion_lat, companion_lon
        if lat is None:
            raise SourceProductError(f"Embedded fire-pixel geolocation is invalid at sparse fire locator {(line, sample)}.")

        record = {
            "source_product": expected,
            "platform": metadata["platform"],
            "instrument": metadata["instrument"],
            "collection": metadata["collection"],
            "processing_version": metadata["processing_version"],
            "manifest_processing_version": metadata["manifest_processing_version"],
            "granule_id": metadata["granule_id"],
            "acquisition_start_utc": metadata["acquisition_start_utc"],
            "acquisition_end_utc": metadata["acquisition_end_utc"],
            "acquisition_time_precision": "granule_start_only" if metadata["acquisition_end_utc"] is None else "granule_interval",
            "pixel_line": line,
            "pixel_sample": sample,
            "latitude": lat,
            "longitude": lon,
            "embedded_latitude": _valid_scalar(embedded_lat),
            "embedded_longitude": _valid_scalar(embedded_lon),
            "geolocation_source": "matched_companion" if geo_lat is not None else "fire_product_embedded_FP_vectors",
            "fire_mask_class": fire_class,
            "algorithm_qa_raw": int(qa_value),
            "geolocation_quality_raw": _plain(geo_quality_row[sample]) if geo_quality is not None else None,
            "confidence_category": {7: "low", 8: "nominal", 9: "high"}[fire_class],
            "daynight": _daynight(_merged_attributes(product_file), field_value("FP_day", i)),
            "daynight_native": field_value("FP_day", i),
            "scan": None,
            "track": None,
            "native_fields": {},
            "source_artifact_sha256": artifact["sha256"],
            "source_artifact_path": artifact["relative_path"],
            "source_record_locator": {"granule_id": metadata["granule_id"], "line": line, "sample": sample},
            "source_artifact_id": artifact["artifact_id"],
            "source_metadata_ref": artifact["artifact_id"],
            "parser_version": PARSER_VERSION,
        }
        for source_name, (target_name, documented_unit) in FIELD_LABELS.items():
            if source_name in fields and fields[source_name] is not None:
                raw = field_value(source_name, i)
                attrs = fields[source_name].attributes or {}
                unit = _text(attrs.get("units")) or documented_unit
                record["native_fields"][target_name] = {"value": raw, "units": unit, "source_dataset": source_name}
                if source_name == "FP_power":
                    record["frp_mw"] = _valid_scalar(raw) if unit and unit.lower() in {"mw", "megawatts", "megawatt"} else None
                elif source_name in {"FP_T21", "FP_T31", "FP_T4", "FP_T5"}:
                    record[target_name] = _valid_scalar(raw) if unit and unit.lower() in {"k", "kelvin"} else None
                elif source_name == "FP_confidence":
                    record["confidence_native"] = raw
        output.append(record)
    return metadata, output


def parse_modis_arrays(product_file: ProductFile, artifact: dict, set_id: str, companion: ProductFile | None = None):
    """Parse MOD14/MYD14 HDF4 reader output using the MODIS L2 fire-pixel table."""
    return _parse_arrays(product_file, artifact, set_id, "modis", companion)


def parse_viirs_arrays(product_file: ProductFile, artifact: dict, set_id: str, companion: ProductFile | None = None):
    """Parse VNP14IMG/VJ114IMG/VJ214IMG NetCDF4/HDF5 reader output."""
    return _parse_arrays(product_file, artifact, set_id, "viirs", companion)


def _hdf4_product(path: Path):
    try:
        from pyhdf.SD import SD, SDC
    except ImportError as exc:
        raise SourceProductError("MOD14/MYD14 parsing requires pyhdf; install project requirements in the active environment.") from exc
    handle = SD(str(path), SDC.READ)
    attrs = dict(handle.attributes())
    datasets = {}
    for name, info in handle.datasets().items():
        sds = handle.select(name)
        dimensions = tuple(info[1])
        ds_attrs = dict(sds.attributes())
        datasets[name] = DatasetView(
            shape=dimensions,
            read_all=lambda sds=sds: sds.get(),
            read_row=(lambda line, sds=sds, width=dimensions[1]: sds.get(start=(line, 0), count=(1, width))) if len(dimensions) == 2 else None,
            attributes=ds_attrs,
        )
    # HDF4 dataset handles live while the SD file is open; retain it on views.
    for view in datasets.values():
        view._handle = handle
    return ProductFile(path=path, attributes=attrs, datasets=datasets)


def _netcdf_product(path: Path):
    try:
        from netCDF4 import Dataset
    except ImportError as exc:
        raise SourceProductError("VIIRS Level-2 parsing requires netCDF4; install project requirements in the active environment.") from exc
    handle = Dataset(str(path), "r")
    attrs = {name: handle.getncattr(name) for name in handle.ncattrs()}
    datasets = {}

    def collect(group, prefix=""):
        for name, variable in group.variables.items():
            full_name = f"{prefix}{name}"
            variable.set_auto_maskandscale(False)
            view = DatasetView(
                shape=tuple(variable.shape),
                read_all=lambda variable=variable: variable[:],
                read_row=(lambda line, variable=variable: variable[line, :]) if variable.ndim == 2 else None,
                attributes={name: variable.getncattr(name) for name in variable.ncattrs()},
            )
            datasets[full_name] = view
            datasets.setdefault(name, view)
        for name, child in group.groups.items():
            collect(child, f"{prefix}{name}/")

    collect(handle)
    for view in datasets.values():
        view._handle = handle
    return ProductFile(path=path, attributes=attrs, datasets=datasets)


def _close_product(product_file):
    if not product_file:
        return
    seen = set()
    for view in product_file.datasets.values():
        handle = getattr(view, "_handle", None)
        if handle is not None and id(handle) not in seen:
            seen.add(id(handle))
            try:
                handle.end() if hasattr(handle, "end") else handle.close()
            except Exception:
                pass


def read_product(path: str | Path, family: str):
    path = Path(path)
    return _hdf4_product(path) if family == "modis" else _netcdf_product(path)


def _expected_companion_product(set_id, role):
    spec = ARTIFACT_SETS[set_id]
    return spec["products"][role]


def _acquisition_token(path):
    match = re.search(r"\.A(\d{4}\d{3})\.(\d{4})\.", path.name)
    return match.groups() if match else None


def _read_metadata_only(path: Path, family: str):
    product = read_product(path, family)
    attrs = _merged_attributes(product)
    result = {
        "short_name": _attr(attrs, "SHORTNAME", "ShortName"),
        "platform": _attr(attrs, "PLATFORM", "PlatformShortName", "platform_short_name"),
        "instrument": _attr(attrs, "INSTRUMENTNAME", "InstrumentShortName", "instrument_short_name"),
        "local_granule_id": _attr(attrs, "LOCALGRANULEID", "LocalGranuleID", "GranuleID"),
        "collection": _attr(attrs, "VERSIONID", "Collection", "collection"),
    }
    _close_product(product)
    return result


def _validate_companion_identity(path, artifact, expected_short_name, family):
    internal = _read_metadata_only(path, family)
    if not internal["short_name"] or expected_short_name not in internal["short_name"]:
        raise SourceProductError(f"Companion internal product ID does not match {expected_short_name}.")
    if internal["platform"] and artifact["platform"].lower() not in internal["platform"].lower():
        raise SourceProductError("Companion internal platform does not match manifest.")
    if internal["instrument"] and internal["instrument"].lower() != artifact["instrument"].lower():
        raise SourceProductError("Companion internal instrument does not match manifest.")
    internal_granule = internal["local_granule_id"]
    if internal_granule and Path(internal_granule).name != path.name:
        raise SourceProductError("Companion internal granule ID does not match original filename.")
    filename = path.name
    token = re.search(r"\.A(\d{4})(\d{3})\.(\d{4})\.(\d{3})\.", filename)
    if not token:
        raise SourceProductError("Companion filename is missing acquisition and collection tokens.")
    year, doy, hhmm, collection = token.groups()
    if collection.lstrip("0") != str(artifact["collection"]).lstrip("0"):
        raise SourceProductError("Companion filename collection does not match the manifest.")
    if datetime.strptime(f"{year}-{doy}", "%Y-%j").date().isoformat() != artifact["acquisition_date_utc"]:
        raise SourceProductError("Companion filename date does not match the manifest.")
    if artifact.get("granule_id") and artifact["granule_id"] != filename:
        raise SourceProductError("Companion filename does not match manifest granule ID.")


def _sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_unique_artifact_hashes(artifacts):
    seen = {}
    for artifact in artifacts:
        checksum = str(artifact.get("sha256", "")).lower()
        if not checksum:
            raise SourceProductError(f"Missing artifact checksum for {artifact.get('artifact_id')}.")
        if checksum in seen:
            raise SourceProductError(f"Duplicate source artifact checksum shared by {seen[checksum]} and {artifact.get('artifact_id')}.")
        seen[checksum] = artifact.get("artifact_id")


def build_run_fingerprint(artifacts, parser_version=PARSER_VERSION):
    payload = {
        "parser_version": parser_version,
        "artifacts": sorted((item["artifact_id"], item["sha256"].lower()) for item in artifacts),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def ingest_manifest(manifest: dict, manifest_path: str | Path, output_dir: str | Path, preview_rows=100):
    """Validate and parse local files to a deterministic CSV/JSON staging bundle."""
    manifest_path = Path(manifest_path).resolve()
    output_dir = Path(output_dir).resolve()
    validation = validate_manifest(manifest, manifest_path)
    if not validation["valid"]:
        raise SourceProductError("Artifact manifest validation failed: " + "; ".join(validation["errors"][:20]))
    root = manifest_path.parent
    validate_unique_artifact_hashes(manifest["artifacts"])
    run_fingerprint = build_run_fingerprint(manifest["artifacts"])
    if output_dir.exists():
        prior = output_dir / "ingestion_manifest.json"
        if prior.is_file():
            try:
                prior_data = json.loads(prior.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                prior_data = {}
            if prior_data.get("run_fingerprint") == run_fingerprint:
                required_outputs = ("observations.csv", "preview.json", "ingestion_manifest.json")
                if all((output_dir / name).is_file() for name in required_outputs):
                    return {"status": "already_staged", "run_fingerprint": run_fingerprint, "output_dir": str(output_dir), "observation_count": prior_data.get("observation_count")}
        raise SourceProductError(f"Output directory exists and is not the same completed run: {output_dir}")

    by_set = {set_id: {} for set_id in ARTIFACT_SETS}
    for artifact in manifest["artifacts"]:
        by_set[artifact["set_id"]][artifact["role"]] = artifact
    observations = []
    group_results = []
    for set_id, roles in by_set.items():
        family = "modis" if roles["fire"]["product_short_name"] in {"MOD14", "MYD14"} else "viirs"
        for role, artifact in roles.items():
            path = (root / artifact["relative_path"]).resolve()
            actual_digest = _sha(path)
            if actual_digest.lower() != artifact["sha256"].lower():
                raise SourceProductError(f"Checksum changed during validation: {artifact['artifact_id']}.")
        fire_artifact = roles["fire"]
        fire_path = (root / fire_artifact["relative_path"]).resolve()
        geo_artifact = roles["geolocation"]
        geo_path = (root / geo_artifact["relative_path"]).resolve()
        _validate_companion_identity(
            geo_path, geo_artifact, _expected_companion_product(set_id, "geolocation"), family
        )
        radiance_path = (root / roles["radiance"]["relative_path"]).resolve()
        _validate_companion_identity(
            radiance_path, roles["radiance"], _expected_companion_product(set_id, "radiance"), family
        )
        for artifact in (fire_artifact, geo_artifact, roles["radiance"]):
            expected_token = _acquisition_token(fire_path)
            if not expected_token or _acquisition_token(root / artifact["relative_path"]) != expected_token:
                raise SourceProductError(f"Companion acquisition token does not match fire granule for {set_id}.")
        fire_file = companion_file = None
        try:
            fire_file = read_product(fire_path, family)
            companion_file = read_product(geo_path, family)
            metadata, rows = (parse_modis_arrays if family == "modis" else parse_viirs_arrays)(
                fire_file, fire_artifact, set_id, companion_file
            )
            observations.extend(rows)
            group_results.append({
                "set_id": set_id,
                "source_product": fire_artifact["product_short_name"],
                "granule_id": metadata["granule_id"],
                "fire_pixel_count": len(rows),
                "observation_outcome": "detections_returned" if rows else "confirmed_zero_detections_for_validated_granule",
                "companion_ids": {role: item["artifact_id"] for role, item in roles.items()},
                "product_metadata": metadata,
                "parser_version": PARSER_VERSION,
            })
        finally:
            _close_product(fire_file)
            _close_product(companion_file)

    staging_manifest = {
        "schema_version": 1,
        "run_fingerprint": run_fingerprint,
        "parser_version": PARSER_VERSION,
        "manifest_path": str(manifest_path),
        "source_artifacts": [{
            "artifact_id": item["artifact_id"], "set_id": item["set_id"],
            "role": item["role"], "sha256": item["sha256"], "relative_path": item["relative_path"],
            "granule_id": item["granule_id"], "product_short_name": item["product_short_name"],
        } for item in manifest["artifacts"]],
        "groups": group_results,
        "observation_count": len(observations),
        "staging_output": "observations.csv",
        "database_imported": False,
        "real_source_compatibility_verified": False,
    }
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent))
    try:
        base_fields = {
            "source_product", "platform", "instrument", "collection", "processing_version",
            "manifest_processing_version", "granule_id", "acquisition_start_utc", "acquisition_end_utc",
            "acquisition_time_precision", "pixel_line", "pixel_sample", "latitude", "longitude",
            "embedded_latitude", "embedded_longitude", "geolocation_source", "fire_mask_class",
            "algorithm_qa_raw", "confidence_category", "confidence_native", "daynight", "daynight_native",
            "geolocation_quality_raw",
            "scan", "track", "frp_mw", "T21_K", "T31_K", "T4_K", "T5_K", "source_artifact_sha256",
            "source_artifact_path", "source_artifact_id", "source_metadata_ref", "parser_version",
        }
        discovered_fields = {key for row in observations for key in row if key not in {"native_fields", "source_record_locator"}}
        fieldnames = sorted(base_fields | discovered_fields)
        fieldnames += ["native_fields_json", "source_record_locator_json"]
        with (temporary / "observations.csv").open("w", newline="", encoding="utf-8") as output:
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            for row in observations:
                serial = {key: value for key, value in row.items() if key in fieldnames}
                serial["native_fields_json"] = json.dumps(row["native_fields"], sort_keys=True)
                serial["source_record_locator_json"] = json.dumps(row["source_record_locator"], sort_keys=True)
                writer.writerow(serial)
        staging_manifest["staged_at_utc"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        (temporary / "ingestion_manifest.json").write_text(json.dumps(staging_manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        preview = observations[:max(0, int(preview_rows))]
        preview_json = []
        for row in preview:
            preview_json.append({key: value for key, value in row.items()})
        (temporary / "preview.json").write_text(json.dumps(preview_json, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, output_dir)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return {"status": "staged", "run_fingerprint": run_fingerprint, "output_dir": str(output_dir), "observation_count": len(observations)}

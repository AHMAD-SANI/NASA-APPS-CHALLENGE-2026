"""Offline preparation and validation for NASA source-artifact pilot manifests.

This module never performs network access or writes to FireDetection. It verifies
local byte integrity and manifest consistency; scientific validation of embedded
HDF/HDF-EOS metadata requires a suitable product reader and remains a separate step.
"""

from __future__ import annotations

import hashlib
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse


PILOT_DATE = "2026-10-18"
PILOT_BBOX = {"west": 110.3, "south": -3.1, "east": 111.0, "north": -2.5}
SCHEMA_VERSION = 1

ARTIFACT_SETS = {
    "terra_modis": {
        "platform": "Terra", "instrument": "MODIS", "collection": "61",
        "products": {"fire": "MOD14", "geolocation": "MOD03", "radiance": "MOD021KM", "metadata": "NASA_GRANULE_METADATA"},
    },
    "aqua_modis": {
        "platform": "Aqua", "instrument": "MODIS", "collection": "61",
        "products": {"fire": "MYD14", "geolocation": "MYD03", "radiance": "MYD021KM", "metadata": "NASA_GRANULE_METADATA"},
    },
    "snpp_viirs": {
        "platform": "Suomi-NPP", "instrument": "VIIRS", "collection": "002",
        "products": {"fire": "VNP14IMG", "geolocation": "VNP03IMG", "radiance": "VNP02IMG", "metadata": "NASA_GRANULE_METADATA"},
    },
    "noaa20_viirs": {
        "platform": "NOAA-20", "instrument": "VIIRS", "collection": "002",
        "products": {"fire": "VJ114IMG", "geolocation": "VJ103IMG", "radiance": "VJ102IMG", "metadata": "NASA_GRANULE_METADATA"},
    },
    "noaa21_viirs": {
        "platform": "NOAA-21", "instrument": "VIIRS", "collection": "002",
        "products": {"fire": "VJ214IMG", "geolocation": "VJ203IMG", "radiance": "VJ202IMG", "metadata": "NASA_GRANULE_METADATA"},
    },
}

REQUIRED_ROLES = ("fire", "geolocation", "radiance", "metadata")
REQUIRED_METADATA_FIELDS = {
    "product_short_name", "platform", "instrument", "collection",
    "processing_version", "granule_id", "acquisition_start_utc", "acquisition_end_utc",
}
NASA_HOSTS = {
    "ladsweb.modaps.eosdis.nasa.gov",
    "lpdaac.usgs.gov",
    "e4ftl01.cr.usgs.gov",
    "cmr.earthdata.nasa.gov",
    "nrt3.modaps.eosdis.nasa.gov",
    "nrt4.modaps.eosdis.nasa.gov",
    "firms.modaps.eosdis.nasa.gov",
    "urs.earthdata.nasa.gov",
}


def manifest_template() -> dict:
    """Return an unacquired manifest template; no granule IDs are invented."""
    return {
        "schema_version": SCHEMA_VERSION,
        "pilot": {
            "cohort_id": "central_kalimantan_20261018",
            "acquisition_date_utc": PILOT_DATE,
            "bbox_wgs84": dict(PILOT_BBOX),
            "status": "awaiting_authorized_acquisition",
        },
        "artifact_sets": [
            {
                "set_id": set_id,
                "platform": spec["platform"],
                "instrument": spec["instrument"],
                "collection": spec["collection"],
                "required_products": dict(spec["products"]),
                "required_roles": list(REQUIRED_ROLES),
                "status": "not_acquired",
            }
            for set_id, spec in ARTIFACT_SETS.items()
        ],
        "artifacts": [],
    }


def _parse_utc(value):
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _format_matches(path: Path, declared: str) -> bool:
    try:
        with path.open("rb") as source:
            head = source.read(16)
    except OSError:
        return False
    kind = declared.strip().lower()
    if kind in {"hdf4", "hdf-eos2"}:
        return head.startswith(b"\x0e\x03\x13\x01")
    if kind in {"hdf5", "netcdf4", "hdf-eos5"}:
        return head.startswith(b"\x89HDF\r\n\x1a\n")
    if kind in {"netcdf3", "cdf"}:
        return head.startswith((b"CDF\x01", b"CDF\x02", b"CDF\x05"))
    if kind == "json":
        try:
            json.loads(path.read_text(encoding="utf-8"))
            return True
        except (OSError, UnicodeError, json.JSONDecodeError):
            return False
    if kind == "xml":
        try:
            ET.parse(path)
            return True
        except (OSError, ET.ParseError):
            return False
    return False


def validate_manifest(manifest: dict, manifest_path: str | Path) -> dict:
    """Validate local files and their declared provenance without network/DB access."""
    root = Path(manifest_path).resolve().parent
    errors = []
    warnings = ["file-internal HDF/HDF-EOS/NetCDF metadata and QA were not parsed by this validator"]
    if not isinstance(manifest, dict) or manifest.get("schema_version") != SCHEMA_VERSION:
        return {"valid": False, "status": "invalid", "errors": ["unsupported_manifest_schema"], "warnings": warnings, "artifact_count": 0, "groups": {}}

    pilot = manifest.get("pilot") or {}
    if pilot.get("acquisition_date_utc") != PILOT_DATE or pilot.get("bbox_wgs84") != PILOT_BBOX:
        errors.append("pilot_scope_mismatch")

    declared_sets = {item.get("set_id"): item for item in manifest.get("artifact_sets", []) if isinstance(item, dict)}
    if set(declared_sets) != set(ARTIFACT_SETS):
        errors.append("artifact_set_inventory_mismatch")
    for set_id, spec in ARTIFACT_SETS.items():
        group = declared_sets.get(set_id)
        if not group:
            continue
        if (
            group.get("platform") != spec["platform"]
            or group.get("instrument") != spec["instrument"]
            or group.get("collection") != spec["collection"]
            or group.get("required_products") != spec["products"]
            or set(group.get("required_roles", [])) != set(REQUIRED_ROLES)
        ):
            errors.append(f"artifact_set_metadata_mismatch:{set_id}")

    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list):
        artifacts = []
        errors.append("artifacts_must_be_a_list")
    by_group = {set_id: {} for set_id in ARTIFACT_SETS}
    artifact_ids = set()
    file_results = []

    for index, artifact in enumerate(artifacts):
        item_errors = []
        if not isinstance(artifact, dict):
            errors.append(f"artifact_not_an_object:{index}")
            continue
        artifact_id = artifact.get("artifact_id")
        group_id = artifact.get("set_id")
        role = artifact.get("role")
        spec = ARTIFACT_SETS.get(group_id)
        if not artifact_id or artifact_id in artifact_ids:
            item_errors.append("missing_or_duplicate_artifact_id")
        artifact_ids.add(artifact_id)
        if spec is None:
            item_errors.append("unknown_artifact_set")
        elif role not in REQUIRED_ROLES or role in by_group[group_id]:
            item_errors.append("unknown_or_duplicate_role")
        else:
            by_group[group_id][role] = artifact
            if artifact.get("product_short_name") != spec["products"][role]:
                item_errors.append("product_role_mismatch")
            if artifact.get("platform") != spec["platform"] or artifact.get("instrument") != spec["instrument"]:
                item_errors.append("platform_instrument_mismatch")
            if artifact.get("collection") != spec["collection"]:
                item_errors.append("collection_mismatch")
        if artifact.get("acquisition_date_utc") != PILOT_DATE:
            item_errors.append("acquisition_date_mismatch")
        if not artifact.get("granule_id") or not artifact.get("relationship_id"):
            item_errors.append("missing_granule_or_relationship_id")
        if not artifact.get("processing_version"):
            item_errors.append("missing_processing_version")
        if not artifact.get("provider"):
            item_errors.append("missing_provider")
        parsed_url = urlparse(artifact.get("archive_url") or "")
        if parsed_url.scheme != "https" or parsed_url.hostname not in NASA_HOSTS:
            item_errors.append("archive_url_not_on_allowed_nasa_host")

        evidence = artifact.get("metadata_evidence")
        if not isinstance(evidence, dict):
            item_errors.append("missing_extracted_metadata_evidence")
        else:
            missing_metadata = REQUIRED_METADATA_FIELDS - set(evidence)
            if missing_metadata:
                item_errors.append("incomplete_extracted_metadata")
            else:
                for field in REQUIRED_METADATA_FIELDS:
                    if evidence.get(field) != artifact.get(field if field != "product_short_name" else "product_short_name"):
                        item_errors.append(f"extracted_metadata_mismatch:{field}")
                        break
                if role == "fire" and not evidence.get("qa_dataset_names"):
                    item_errors.append("fire_qa_datasets_not_recorded")

        relative = artifact.get("relative_path")
        if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
            item_errors.append("invalid_relative_path")
            resolved = None
        else:
            resolved = (root / relative).resolve()
            if resolved != root and root not in resolved.parents:
                item_errors.append("artifact_path_escapes_manifest_root")
                resolved = None
        if resolved is not None:
            if resolved.name.endswith(".part") or not resolved.is_file():
                item_errors.append("artifact_missing_or_incomplete")
            else:
                expected_size = artifact.get("size_bytes")
                if not isinstance(expected_size, int) or expected_size <= 0 or resolved.stat().st_size != expected_size:
                    item_errors.append("size_mismatch")
                expected_hash = artifact.get("sha256")
                if not isinstance(expected_hash, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", expected_hash):
                    item_errors.append("invalid_or_missing_sha256")
                elif _sha256(resolved).lower() != expected_hash.lower():
                    item_errors.append("sha256_mismatch")
                if not _format_matches(resolved, artifact.get("format", "")):
                    item_errors.append("file_format_signature_mismatch")

        start = _parse_utc(artifact.get("acquisition_start_utc"))
        end = _parse_utc(artifact.get("acquisition_end_utc"))
        if start is None or end is None or end < start or start.date().isoformat() != PILOT_DATE:
            item_errors.append("invalid_acquisition_interval")
        file_results.append({"artifact_id": artifact_id, "set_id": group_id, "role": role, "errors": item_errors})
        errors.extend(f"{artifact_id or index}:{code}" for code in item_errors)

    group_results = {}
    for set_id, role_map in by_group.items():
        missing_roles = sorted(set(REQUIRED_ROLES) - set(role_map))
        group_errors = []
        if missing_roles:
            group_errors.append("missing_companion_roles:" + ",".join(missing_roles))
        relationship_ids = {item.get("relationship_id") for item in role_map.values()}
        if len(role_map) == len(REQUIRED_ROLES) and len(relationship_ids) != 1:
            group_errors.append("companion_relationship_id_mismatch")
        group_results[set_id] = {"status": "incomplete" if group_errors else "manifest_consistent_pending_internal_parser", "missing_roles": missing_roles, "errors": group_errors}
        errors.extend(f"{set_id}:{code}" for code in group_errors)

    complete = all(not result["missing_roles"] and not result["errors"] for result in group_results.values())
    valid = not errors and complete
    return {
        "valid": valid,
        "status": "manifest_consistent_pending_internal_parser" if valid else "incomplete_or_invalid",
        "science_validated": False,
        "errors": errors,
        "warnings": warnings,
        "artifact_count": len(artifacts),
        "file_results": file_results,
        "groups": group_results,
    }


def write_manifest_template(output_path: str | Path) -> Path:
    """Create a new local-only template without overwriting an existing manifest."""
    path = Path(output_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(f"Manifest already exists: {path}")
    path.write_text(json.dumps(manifest_template(), indent=2) + "\n", encoding="utf-8")
    return path

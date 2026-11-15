"""Synthetic schema fixtures; these are not NASA source granules."""

import hashlib
import json
import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

import numpy as np

from fires.source_artifacts import ARTIFACT_SETS, REQUIRED_ROLES, manifest_template
from fires.source_ingestion import (
    DatasetView,
    ProductFile,
    SourceProductError,
    build_run_fingerprint,
    parse_modis_arrays,
    parse_viirs_arrays,
    ingest_manifest,
    validate_unique_artifact_hashes,
)


def _view(array, attributes=None):
    data = np.asanyarray(array)
    return DatasetView(
        shape=tuple(data.shape),
        read_all=lambda data=data: data,
        read_row=(lambda row, data=data: data[row, :]) if data.ndim == 2 else None,
        attributes=attributes or {},
    )


def _artifact(set_id, short_name, collection):
    spec = ARTIFACT_SETS[set_id]
    prefix = short_name
    granule = f'{prefix}.A2026291.1200.{collection}.2026291120600.nc'
    return {
        'artifact_id': f'{set_id}-fire', 'set_id': set_id, 'role': 'fire',
        'product_short_name': short_name, 'platform': spec['platform'], 'instrument': spec['instrument'],
        'collection': spec['collection'], 'processing_version': 'fixture-processing-version',
        'granule_id': granule, 'relationship_id': f'{set_id}-relationship',
        'acquisition_date_utc': '2026-10-18', 'relative_path': f'original/{granule}',
        'sha256': 'a' * 64,
    }


def _synthetic_file(artifact, fire_mask=None, qa=None, fields=None, attrs_extra=None):
    attrs = {
        'ShortName': artifact['product_short_name'],
        'PlatformShortName': artifact['platform'],
        'InstrumentShortName': artifact['instrument'],
        'LocalGranuleID': artifact['granule_id'],
        'RangeBeginningDate': '2026-10-18',
        'RangeBeginningTime': '12:00:00.000Z',
        'RangeEndingDate': '2026-10-18',
        'RangeEndingTime': '12:06:00.000Z',
        'FirePix': 2,
        **(attrs_extra or {}),
    }
    arrays = {
        'fire mask': _view(fire_mask if fire_mask is not None else [[5, 8, 5], [5, 5, 9]]),
        'algorithm QA': _view(qa if qa is not None else [[0, 12345, 0], [0, 0, 67890]]),
    }
    for name, value in (fields or {}).items():
        arrays[name] = _view(value)
    return ProductFile(Path(artifact['granule_id']), attrs, arrays)


def _modis_fields():
    return {
        'FP_line': [0, 1], 'FP_sample': [1, 2],
        'FP_latitude': [1.25, 1.5], 'FP_longitude': [101.25, 101.5],
        'FP_T21': [321.5, 350.2], 'FP_T31': [299.2, 300.5],
        'FP_power': [12.0, 20.5], 'FP_confidence': [42, 96],
    }


def _viirs_fields():
    return {
        'FP_line': [0, 1], 'FP_sample': [1, 2],
        'FP_latitude': [1.25, 1.5], 'FP_longitude': [101.25, 101.5],
        'FP_T4': [321.5, 350.2], 'FP_T5': [299.2, 300.5],
        'FP_power': [12.0, 20.5], 'FP_confidence': [8, 9], 'FP_day': [0, 1],
    }


class SourceProductParserTests(TestCase):
    def test_mod14_and_myd14_extract_native_values_and_zero_based_locators(self):
        for set_id, product, platform in [('terra_modis', 'MOD14', 'Terra'), ('aqua_modis', 'MYD14', 'Aqua')]:
            artifact = _artifact(set_id, product, '061')
            source = _synthetic_file(artifact, fields=_modis_fields(), attrs_extra={'DayNightFlag': 'Night'})
            metadata, records = parse_modis_arrays(source, artifact, set_id)
            self.assertEqual(metadata['platform'], platform)
            self.assertEqual([r['source_record_locator']['line'] for r in records], [0, 1])
            self.assertEqual([r['source_record_locator']['sample'] for r in records], [1, 2])
            self.assertEqual(records[0]['frp_mw'], 12.0)
            self.assertEqual(records[0]['T21_K'], 321.5)
            self.assertEqual(records[0]['algorithm_qa_raw'], 12345)
            self.assertEqual(records[0]['daynight'], 'N')
            self.assertIsNone(records[0]['scan'])
            self.assertIsNone(records[0]['track'])

    def test_all_three_viirs_products_keep_platform_and_native_confidence(self):
        products = [
            ('snpp_viirs', 'VNP14IMG', 'Suomi-NPP'),
            ('noaa20_viirs', 'VJ114IMG', 'NOAA-20'),
            ('noaa21_viirs', 'VJ214IMG', 'NOAA-21'),
        ]
        for set_id, product, platform in products:
            artifact = _artifact(set_id, product, '002')
            source = _synthetic_file(artifact, fire_mask=[[5, 8, 5], [5, 5, 9]], fields=_viirs_fields())
            metadata, records = parse_viirs_arrays(source, artifact, set_id)
            self.assertEqual(metadata['platform'], platform)
            self.assertEqual(records[0]['confidence_native'], 8)
            self.assertEqual(records[0]['confidence_category'], 'nominal')
            self.assertEqual(records[1]['confidence_category'], 'high')
            self.assertIsNone(records[0]['daynight'])  # numeric polarity is not assumed
            self.assertEqual(records[0]['daynight_native'], 0)
            self.assertEqual(records[0]['T4_K'], 321.5)

    def test_companion_geolocation_is_indexed_by_line_and_sample(self):
        artifact = _artifact('snpp_viirs', 'VNP14IMG', '002')
        fire = _synthetic_file(artifact, fields=_viirs_fields())
        geo_attrs = {'ShortName': 'VNP03IMG', 'PlatformShortName': 'Suomi-NPP', 'InstrumentShortName': 'VIIRS'}
        companion = ProductFile(Path('VNP03IMG.A2026291.1200.002.2026291120600.nc'), geo_attrs, {
            'geolocation_data/latitude': _view([[0.0, 10.0, 0.0], [0.0, 0.0, 20.0]]),
            'geolocation_data/longitude': _view([[0.0, 110.0, 0.0], [0.0, 0.0, 120.0]]),
            'geolocation_data/quality_flag': _view([[0, 1, 0], [0, 0, 2]]),
        })
        _, records = parse_viirs_arrays(fire, artifact, 'snpp_viirs', companion)
        self.assertEqual(records[0]['latitude'], 10.0)
        self.assertEqual(records[0]['longitude'], 110.0)
        self.assertEqual(records[0]['embedded_latitude'], 1.25)
        self.assertEqual(records[0]['geolocation_source'], 'matched_companion')
        self.assertEqual(records[0]['geolocation_quality_raw'], 1)

    def test_missing_optional_brightness_stays_missing(self):
        artifact = _artifact('terra_modis', 'MOD14', '061')
        fields = _modis_fields()
        fields.pop('FP_T31')
        _, records = parse_modis_arrays(_synthetic_file(artifact, fields=fields), artifact, 'terra_modis')
        self.assertNotIn('T31_K', records[0])
        self.assertEqual(records[0]['native_fields'].get('T31_K'), None)

    def test_mismatched_dimensions_and_out_of_bounds_index_rejected(self):
        artifact = _artifact('noaa21_viirs', 'VJ214IMG', '002')
        source = _synthetic_file(artifact, qa=[[1, 2]], fields=_viirs_fields())
        with self.assertRaisesRegex(SourceProductError, 'identical shapes'):
            parse_viirs_arrays(source, artifact, 'noaa21_viirs')
        fields = _viirs_fields()
        fields['FP_sample'] = [1, 3]
        source = _synthetic_file(artifact, fields=fields)
        with self.assertRaisesRegex(SourceProductError, 'outside mask dimensions'):
            parse_viirs_arrays(source, artifact, 'noaa21_viirs')

    def test_nonfire_sparse_location_and_invalid_coordinate_rejected(self):
        artifact = _artifact('terra_modis', 'MOD14', '061')
        fields = _modis_fields()
        fields['FP_latitude'] = [91.0, 1.5]
        source = _synthetic_file(artifact, fields=fields)
        with self.assertRaisesRegex(SourceProductError, 'geolocation is invalid'):
            parse_modis_arrays(source, artifact, 'terra_modis')
        source = _synthetic_file(artifact, fire_mask=[[5, 5, 5], [5, 5, 9]], fields=_modis_fields())
        with self.assertRaisesRegex(SourceProductError, 'non-fire mask class'):
            parse_modis_arrays(source, artifact, 'terra_modis')

    def test_zero_fire_product_requires_and_respects_firepix_attribute(self):
        artifact = _artifact('noaa20_viirs', 'VJ114IMG', '002')
        fields = {name: [] for name in _viirs_fields()}
        source = _synthetic_file(artifact, fire_mask=np.full((2, 3), 5), qa=np.zeros((2, 3)), fields=fields, attrs_extra={'FirePix': 0})
        _, records = parse_viirs_arrays(source, artifact, 'noaa20_viirs')
        self.assertEqual(records, [])
        source.attributes.pop('FirePix')
        with self.assertRaisesRegex(SourceProductError, 'FirePix count attribute'):
            parse_viirs_arrays(source, artifact, 'noaa20_viirs')

    def test_duplicate_checksums_and_fingerprint_reproducibility(self):
        artifacts = [{'artifact_id': 'one', 'sha256': 'a' * 64}, {'artifact_id': 'two', 'sha256': 'b' * 64}]
        self.assertEqual(build_run_fingerprint(artifacts), build_run_fingerprint(list(reversed(artifacts))))
        with self.assertRaisesRegex(SourceProductError, 'Duplicate source artifact checksum'):
            validate_unique_artifact_hashes([artifacts[0], {'artifact_id': 'copy', 'sha256': 'a' * 64}])

    def test_complete_synthetic_pilot_stages_once_and_reuses_identical_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'cohort'
            root.mkdir()
            manifest_path = root / 'manifest.json'
            manifest = manifest_template()
            fake_files = {}
            artifact_specs = []
            for set_id, spec in ARTIFACT_SETS.items():
                family = 'modis' if spec['instrument'] == 'MODIS' else 'viirs'
                collection_token = '061' if family == 'modis' else '002'
                extension = 'hdf' if family == 'modis' else 'nc'
                fire_filename = f"{spec['products']['fire']}.A2026291.1200.{collection_token}.2026291120600.{extension}"
                for role in REQUIRED_ROLES:
                    product = spec['products'][role]
                    filename = fire_filename if role == 'fire' else (f'{set_id}_metadata.json' if role == 'metadata' else f"{product}.A2026291.1200.{collection_token}.2026291120600.{extension}")
                    content = json.dumps({'artifact': f'{set_id}-{role}'}).encode() if role == 'metadata' else (b'\x0e\x03\x13\x01' if family == 'modis' else b'\x89HDF\r\n\x1a\n') + f'{set_id}-{role}'.encode()
                    file_path = root / 'original' / filename
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                    file_path.write_bytes(content)
                    artifact_id = f'{set_id}-{role}'
                    granule_id = filename
                    evidence = {
                        'product_short_name': product, 'platform': spec['platform'], 'instrument': spec['instrument'],
                        'collection': spec['collection'], 'processing_version': 'fixture', 'granule_id': granule_id,
                        'acquisition_start_utc': '2026-10-18T12:00:00Z', 'acquisition_end_utc': '2026-10-18T12:06:00Z',
                    }
                    if role == 'fire':
                        evidence['qa_dataset_names'] = ['synthetic fixture QA array']
                    artifact = {
                        'artifact_id': artifact_id, 'set_id': set_id, 'role': role, 'provider': 'NASA LAADS DAAC',
                        'product_short_name': product, 'platform': spec['platform'], 'instrument': spec['instrument'],
                        'collection': spec['collection'], 'processing_version': 'fixture', 'granule_id': granule_id,
                        'relationship_id': set_id, 'acquisition_date_utc': '2026-10-18',
                        'acquisition_start_utc': '2026-10-18T12:00:00Z', 'acquisition_end_utc': '2026-10-18T12:06:00Z',
                        'archive_url': 'https://ladsweb.modaps.eosdis.nasa.gov/archive/fixture',
                        'original_filename': filename, 'relative_path': str(file_path.relative_to(root)),
                        'size_bytes': len(content), 'sha256': hashlib.sha256(content).hexdigest(),
                        'format': 'json' if role == 'metadata' else ('hdf4' if family == 'modis' else 'hdf5'),
                        'metadata_evidence': evidence,
                    }
                    artifact_specs.append(artifact)
                    if role != 'metadata':
                        fake_files[file_path.resolve()] = (role, set_id, family, artifact)
            manifest['artifacts'] = artifact_specs
            manifest_path.write_text(json.dumps(manifest), encoding='utf-8')

            def fake_reader(path, family):
                role, set_id, file_family, artifact = fake_files[Path(path).resolve()]
                self.assertEqual(file_family, family)
                if role == 'fire':
                    fields = _modis_fields() if family == 'modis' else _viirs_fields()
                    return _synthetic_file(artifact, fields=fields)
                attrs = {
                    'ShortName': artifact['product_short_name'], 'PlatformShortName': artifact['platform'],
                    'InstrumentShortName': artifact['instrument'], 'LocalGranuleID': Path(path).name,
                }
                if role == 'geolocation':
                    names = ('Latitude', 'Longitude') if family == 'modis' else ('geolocation_data/latitude', 'geolocation_data/longitude')
                    return ProductFile(Path(path), attrs, {
                        names[0]: _view([[0.0, 1.25, 0.0], [0.0, 0.0, 1.5]]),
                        names[1]: _view([[0.0, 101.25, 0.0], [0.0, 0.0, 101.5]]),
                        ('gflags' if family == 'modis' else 'geolocation_data/quality_flag'): _view([[0, 1, 0], [0, 0, 2]]),
                    })
                return ProductFile(Path(path), attrs, {})

            output_dir = Path(directory) / 'staging'
            with patch('fires.source_ingestion.read_product', side_effect=fake_reader):
                first = ingest_manifest(manifest, manifest_path, output_dir)
                second = ingest_manifest(manifest, manifest_path, output_dir)
            self.assertEqual(first['status'], 'staged')
            self.assertEqual(first['observation_count'], 10)
            self.assertEqual(second['status'], 'already_staged')
            staged_manifest = json.loads((output_dir / 'ingestion_manifest.json').read_text(encoding='utf-8'))
            self.assertFalse(staged_manifest['database_imported'])
            self.assertEqual(len(staged_manifest['groups']), 5)

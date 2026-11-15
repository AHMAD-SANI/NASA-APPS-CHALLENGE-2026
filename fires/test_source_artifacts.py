import hashlib
import json
import tempfile
from pathlib import Path
from unittest import TestCase

from fires.source_artifacts import (
    ARTIFACT_SETS,
    manifest_template,
    validate_manifest,
    write_manifest_template,
)


class SourceArtifactManifestTests(TestCase):
    def test_template_has_five_platform_sets_without_invented_granules(self):
        template = manifest_template()
        self.assertEqual(len(template['artifact_sets']), 5)
        self.assertEqual(template['artifacts'], [])
        self.assertTrue(all(item['status'] == 'not_acquired' for item in template['artifact_sets']))

    def test_template_writer_does_not_overwrite_existing_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'manifest.json'
            write_manifest_template(path)
            original = path.read_text(encoding='utf-8')
            with self.assertRaises(FileExistsError):
                write_manifest_template(path)
            self.assertEqual(path.read_text(encoding='utf-8'), original)

    def test_empty_pilot_manifest_reports_all_companions_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            result = validate_manifest(manifest_template(), Path(directory) / 'manifest.json')
        json.dumps(result)
        self.assertFalse(result['valid'])
        self.assertEqual(result['status'], 'incomplete_or_invalid')
        self.assertTrue(all(len(group['missing_roles']) == 4 for group in result['groups'].values()))

    def test_complete_local_manifest_checks_hash_size_format_and_metadata_consistency(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = manifest_template()
            for set_id, spec in ARTIFACT_SETS.items():
                for role, short_name in spec['products'].items():
                    artifact_id = f'{set_id}-{role}'
                    if role == 'metadata':
                        content = json.dumps({'test_fixture': True}).encode('utf-8')
                        file_format = 'json'
                    else:
                        content = b'\x89HDF\r\n\x1a\n' + b'test-fixture'
                        file_format = 'hdf5'
                    path = root / f'{artifact_id}.{file_format}'
                    path.write_bytes(content)
                    evidence = {
                        'product_short_name': short_name,
                        'platform': spec['platform'],
                        'instrument': spec['instrument'],
                        'collection': spec['collection'],
                        'processing_version': 'fixture-version',
                        'granule_id': f'fixture-granule-{set_id}',
                        'acquisition_start_utc': '2026-10-18T10:00:00Z',
                        'acquisition_end_utc': '2026-10-18T10:06:00Z',
                    }
                    if role == 'fire':
                        evidence['qa_dataset_names'] = ['fixture QA dataset; test only']
                    manifest['artifacts'].append({
                        'artifact_id': artifact_id,
                        'set_id': set_id,
                        'role': role,
                        'provider': 'NASA LAADS DAAC',
                        'product_short_name': short_name,
                        'platform': spec['platform'],
                        'instrument': spec['instrument'],
                        'collection': spec['collection'],
                        'processing_version': 'fixture-version',
                        'granule_id': f'fixture-granule-{set_id}',
                        'relationship_id': f'fixture-acquisition-{set_id}',
                        'acquisition_date_utc': '2026-10-18',
                        'acquisition_start_utc': '2026-10-18T10:00:00Z',
                        'acquisition_end_utc': '2026-10-18T10:06:00Z',
                        'archive_url': 'https://ladsweb.modaps.eosdis.nasa.gov/archive/fixture',
                        'original_filename': path.name,
                        'relative_path': path.name,
                        'size_bytes': len(content),
                        'sha256': hashlib.sha256(content).hexdigest(),
                        'format': file_format,
                        'metadata_evidence': evidence,
                    })

            result = validate_manifest(manifest, root / 'manifest.json')
        self.assertTrue(result['valid'], result['errors'])
        self.assertFalse(result['science_validated'])
        self.assertEqual(result['artifact_count'], 20)
        self.assertTrue(all(group['status'] == 'manifest_consistent_pending_internal_parser' for group in result['groups'].values()))

    def test_hash_size_and_product_mismatches_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = manifest_template()
            spec = ARTIFACT_SETS['terra_modis']
            artifact = {
                'artifact_id': 'terra-fire', 'set_id': 'terra_modis', 'role': 'fire',
                'provider': 'NASA LAADS DAAC', 'product_short_name': 'MYD14',
                'platform': 'Terra', 'instrument': 'MODIS', 'collection': '61',
                'processing_version': 'fixture', 'granule_id': 'fixture-granule',
                'relationship_id': 'fixture-acquisition', 'acquisition_date_utc': '2026-10-18',
                'acquisition_start_utc': '2026-10-18T10:00:00Z', 'acquisition_end_utc': '2026-10-18T10:06:00Z',
                'archive_url': 'https://ladsweb.modaps.eosdis.nasa.gov/archive/fixture',
                'original_filename': 'fixture.h5', 'relative_path': 'fixture.h5',
                'size_bytes': 4, 'sha256': '0' * 64, 'format': 'hdf5',
                'metadata_evidence': {
                    'product_short_name': 'MYD14', 'platform': 'Terra', 'instrument': 'MODIS',
                    'collection': '61', 'processing_version': 'fixture', 'granule_id': 'fixture-granule',
                    'acquisition_start_utc': '2026-10-18T10:00:00Z', 'acquisition_end_utc': '2026-10-18T10:06:00Z',
                    'qa_dataset_names': ['fixture QA dataset; test only'],
                },
            }
            (root / 'fixture.h5').write_bytes(b'bad')
            manifest['artifacts'] = [artifact]
            result = validate_manifest(manifest, root / 'manifest.json')
        codes = set(result['errors'])
        self.assertTrue(any('product_role_mismatch' in code for code in codes))
        self.assertTrue(any('size_mismatch' in code for code in codes))
        self.assertTrue(any('sha256_mismatch' in code for code in codes))
        self.assertTrue(any('file_format_signature_mismatch' in code for code in codes))

    def test_path_traversal_and_missing_companion_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as outside_directory:
            root = Path(directory)
            outside = Path(outside_directory) / 'outside.h5'
            outside.write_bytes(b'\x89HDF\r\n\x1a\n')
            manifest = manifest_template()
            spec = ARTIFACT_SETS['snpp_viirs']
            manifest['artifacts'] = [{
                'artifact_id': 'snpp-fire', 'set_id': 'snpp_viirs', 'role': 'fire',
                'provider': 'NASA LAADS DAAC', 'product_short_name': spec['products']['fire'],
                'platform': spec['platform'], 'instrument': spec['instrument'], 'collection': spec['collection'],
                'processing_version': 'fixture', 'granule_id': 'fixture', 'relationship_id': 'fixture',
                'acquisition_date_utc': '2026-10-18', 'acquisition_start_utc': '2026-10-18T10:00:00Z',
                'acquisition_end_utc': '2026-10-18T10:06:00Z',
                'archive_url': 'https://ladsweb.modaps.eosdis.nasa.gov/archive/fixture',
                'original_filename': 'outside.h5', 'relative_path': '../outside.h5', 'size_bytes': outside.stat().st_size,
                'sha256': hashlib.sha256(outside.read_bytes()).hexdigest(), 'format': 'hdf5',
                'metadata_evidence': {
                    'product_short_name': spec['products']['fire'], 'platform': spec['platform'],
                    'instrument': spec['instrument'], 'collection': spec['collection'], 'processing_version': 'fixture',
                    'granule_id': 'fixture', 'acquisition_start_utc': '2026-10-18T10:00:00Z',
                    'acquisition_end_utc': '2026-10-18T10:06:00Z', 'qa_dataset_names': ['fixture QA; test only'],
                },
            }]
            result = validate_manifest(manifest, root / 'manifest.json')
        self.assertTrue(any('artifact_path_escapes_manifest_root' in code for code in result['errors']))
        self.assertTrue(any('missing_companion_roles' in code for code in result['errors']))

    def test_artifact_from_adjacent_utc_date_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = manifest_template()
            spec = ARTIFACT_SETS['terra_modis']
            content = b'\x89HDF\r\n\x1a\nfixture'
            path = root / 'terra-fire.h5'
            path.write_bytes(content)
            manifest['artifacts'] = [{
                'artifact_id': 'terra-fire', 'set_id': 'terra_modis', 'role': 'fire',
                'provider': 'NASA LAADS DAAC', 'product_short_name': spec['products']['fire'],
                'platform': spec['platform'], 'instrument': spec['instrument'], 'collection': spec['collection'],
                'processing_version': 'fixture', 'granule_id': 'fixture', 'relationship_id': 'fixture',
                'acquisition_date_utc': '2026-10-19', 'acquisition_start_utc': '2026-10-19T00:00:00Z',
                'acquisition_end_utc': '2026-10-19T00:06:00Z',
                'archive_url': 'https://ladsweb.modaps.eosdis.nasa.gov/archive/fixture',
                'original_filename': path.name, 'relative_path': path.name, 'size_bytes': len(content),
                'sha256': hashlib.sha256(content).hexdigest(), 'format': 'hdf5',
                'metadata_evidence': {
                    'product_short_name': spec['products']['fire'], 'platform': spec['platform'],
                    'instrument': spec['instrument'], 'collection': spec['collection'], 'processing_version': 'fixture',
                    'granule_id': 'fixture', 'acquisition_start_utc': '2026-10-19T00:00:00Z',
                    'acquisition_end_utc': '2026-10-19T00:06:00Z', 'qa_dataset_names': ['fixture QA; test only'],
                },
            }]
            result = validate_manifest(manifest, root / 'manifest.json')
        self.assertTrue(any('acquisition_date_mismatch' in code for code in result['errors']))
        self.assertTrue(any('invalid_acquisition_interval' in code for code in result['errors']))

import tempfile
from pathlib import Path
from unittest import mock

import requests
from django.test import SimpleTestCase

from fires import firms_archive as archive

HEADER = 'latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,instrument,confidence,version,bright_t31,frp,daynight,type\n'
ROW = '9.85,5.57,304.6,2.8,1.6,{day},0147,Aqua,MODIS,61,6.2,291.4,30.6,N,0\n'


def csv_bytes(*days):
    return (HEADER + ''.join(ROW.format(day=day) for day in days)).encode()


class FakeResponse:
    def __init__(self, body=b'', status=200, declared_length=None):
        self.body, self.status_code = body, status
        self.headers = {'Content-Length': str(len(body) if declared_length is None else declared_length)}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def iter_content(self, chunk_size):
        yield self.body

    def close(self):
        pass


class FetchYearTests(SimpleTestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.manifest = {'files': {}}
        self.session = mock.Mock()

    def fetch(self, **kwargs):
        return archive.fetch_year(self.root, self.manifest, 'modis', 2005, 'Nigeria', self.session, sleep=lambda _: None, **kwargs)

    def target(self):
        return self.root / archive.relative_path('modis', 2005, 'Nigeria')

    def test_verified_file_is_profiled_and_kept(self):
        self.session.get.return_value = FakeResponse(csv_bytes('2005-01-01', '2005-01-01', '2005-03-02'))
        entry = self.fetch()
        self.assertEqual(entry['status'], 'verified')
        self.assertEqual((entry['rows'], entry['days_with_detections']), (3, 2))
        self.assertEqual((entry['first_date'], entry['last_date']), ('2005-01-01', '2005-03-02'))
        self.assertEqual(entry['types'], {'0': 3})
        self.assertTrue(self.target().exists())

    def test_verified_file_is_not_downloaded_twice(self):
        self.session.get.return_value = FakeResponse(csv_bytes('2005-01-01'))
        self.fetch()
        self.assertTrue(self.fetch()['skipped'])
        self.assertEqual(self.session.get.call_count, 1)

    def test_corrupted_local_file_is_downloaded_again(self):
        self.session.get.side_effect = lambda *a, **k: FakeResponse(csv_bytes('2005-01-01'))
        self.fetch()
        self.target().write_bytes(b'corrupted')
        self.assertEqual(self.fetch()['status'], 'verified')
        self.assertEqual(self.session.get.call_count, 2)

    def test_missing_year_is_unavailable_without_retry(self):
        self.session.get.return_value = FakeResponse(status=404)
        self.assertEqual(self.fetch()['status'], 'unavailable')
        self.assertEqual(self.session.get.call_count, 1)

    def test_truncated_download_retries_then_fails_and_keeps_no_file(self):
        self.session.get.side_effect = lambda *a, **k: FakeResponse(csv_bytes('2005-01-01'), declared_length=9999)
        self.assertEqual(self.fetch()['status'], 'failed')
        self.assertEqual(self.session.get.call_count, archive.MAX_ATTEMPTS)
        self.assertEqual(list(self.target().parent.iterdir()), [])

    def test_wrong_columns_are_invalid(self):
        self.session.get.return_value = FakeResponse(b'a,b\n1,2\n')
        self.assertEqual(self.fetch()['status'], 'invalid')
        self.assertFalse(self.target().exists())

    def test_header_only_file_is_invalid(self):
        self.session.get.return_value = FakeResponse(HEADER.encode())
        self.assertEqual(self.fetch()['status'], 'invalid')

    def test_rows_from_another_year_are_invalid(self):
        self.session.get.return_value = FakeResponse(csv_bytes('2006-01-01'))
        self.assertEqual(self.fetch()['status'], 'invalid')

    def test_failed_refetch_keeps_existing_verified_record(self):
        self.session.get.return_value = FakeResponse(csv_bytes('2005-01-01'))
        good = self.fetch()
        self.session.get.return_value = FakeResponse(status=404)
        self.fetch(force=True)
        self.assertEqual(self.manifest['files']['Nigeria/modis/2005']['sha256'], good['sha256'])
        self.assertTrue(self.target().exists())

    def test_country_name_cannot_escape_the_archive(self):
        for name in ('../evil', 'a/b', ''):
            with self.assertRaises(ValueError):
                archive.validate_country(name)

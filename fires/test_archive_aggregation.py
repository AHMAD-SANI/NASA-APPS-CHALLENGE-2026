import tempfile
from datetime import date
from pathlib import Path

from django.test import SimpleTestCase, TestCase

from fires import archive_aggregation as aggregation
from fires import firms_archive as archive
from fires.models import ArchiveCellDay, ArchiveFile

HEADER = 'latitude,longitude,acq_date,acq_time,satellite,instrument,confidence,version,frp,daynight,type\n'


def row(lat='9.10', lon='5.60', day='2005-01-01', satellite='Terra', confidence='61', frp='10.5', daynight='D', kind='0'):
    return f'{lat},{lon},{day},0147,{satellite},MODIS,{confidence},6.2,{frp},{daynight},{kind}\n'


class ArchiveDirectoryMixin:
    def write(self, *rows, name='file.csv'):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / name
        path.write_text(HEADER + ''.join(rows), encoding='utf-8')
        return path


class AggregateCsvTests(ArchiveDirectoryMixin, SimpleTestCase):
    def test_grid_index_floors_negative_coordinates(self):
        self.assertEqual(aggregation.grid_index(9.1), 36)
        self.assertEqual(aggregation.grid_index(-0.1), -1)
        self.assertEqual(aggregation.grid_index(-0.25), -1)

    def test_detections_in_one_cell_and_day_are_summed(self):
        result = aggregation.aggregate_csv(self.write(row(), row(lat='9.20', confidence='20', frp='1.5', daynight='N')), 2005)
        totals = result.cells[('MODIS_TERRA', date(2005, 1, 1), 36, 22)]
        self.assertEqual((totals.detections, totals.daytime, totals.nighttime, totals.low_confidence), (2, 1, 1, 1))
        self.assertAlmostEqual(totals.frp_sum, 12.0)

    def test_terra_and_aqua_stay_separate(self):
        result = aggregation.aggregate_csv(self.write(row(), row(satellite='Aqua')), 2005)
        self.assertEqual({key[0] for key in result.cells}, {'MODIS_TERRA', 'MODIS_AQUA'})

    def test_viirs_low_confidence_uses_letter_class(self):
        result = aggregation.aggregate_csv(self.write(row(satellite='N', confidence='l'), row(satellite='N', confidence='n')), 2005)
        self.assertEqual(next(iter(result.cells.values())).low_confidence, 1)

    def test_non_vegetation_rows_are_counted_but_not_aggregated(self):
        result = aggregation.aggregate_csv(self.write(row(), row(kind='2'), row(kind='3')), 2005)
        self.assertEqual(result.vegetation_rows, 1)
        self.assertEqual(dict(result.non_vegetation), {'2': 1, '3': 1})

    def test_bad_and_out_of_year_rows_are_rejected(self):
        result = aggregation.aggregate_csv(self.write(row(), row(lat='x'), row(day='2006-01-01')), 2005)
        self.assertEqual((result.vegetation_rows, result.rejected_rows), (1, 2))

    def test_unknown_satellite_stops_the_file(self):
        with self.assertRaises(aggregation.AggregationError):
            aggregation.aggregate_csv(self.write(row(satellite='Mystery')), 2005)

    def test_partial_year_is_not_complete(self):
        partial = aggregation.aggregate_csv(self.write(row(day='2005-11-01'), row(day='2005-12-31')), 2005)
        full = aggregation.aggregate_csv(self.write(row(day='2005-01-02'), row(day='2005-12-30')), 2005)
        self.assertFalse(aggregation.platform_stats(partial, 2005)['MODIS_TERRA']['complete_year'])
        self.assertTrue(aggregation.platform_stats(full, 2005)['MODIS_TERRA']['complete_year'])


class AggregateEntryTests(ArchiveDirectoryMixin, TestCase):
    def entry(self, path, rows):
        return {
            'country': 'Nigeria', 'product': 'modis', 'year': 2005, 'path': path.name,
            'sha256': archive.sha256_of(path), 'rows': rows,
        }

    def test_rerun_is_skipped_and_changed_file_replaces_old_rows(self):
        path = self.write(row(), row(lat='12.0'))
        self.assertEqual(aggregation.aggregate_manifest_entry(path.parent, self.entry(path, 2))[0], 'aggregated')
        self.assertEqual(aggregation.aggregate_manifest_entry(path.parent, self.entry(path, 2))[0], 'skipped')
        self.assertEqual(ArchiveCellDay.objects.count(), 2)

        path.write_text(HEADER + row(), encoding='utf-8')
        aggregation.aggregate_manifest_entry(path.parent, self.entry(path, 1))
        self.assertEqual(ArchiveFile.objects.count(), 1)
        self.assertEqual(ArchiveCellDay.objects.count(), 1)

    def test_checksum_mismatch_writes_nothing(self):
        path = self.write(row())
        entry = {**self.entry(path, 1), 'sha256': 'wrong'}
        with self.assertRaises(aggregation.AggregationError):
            aggregation.aggregate_manifest_entry(path.parent, entry)
        self.assertFalse(ArchiveFile.objects.exists())

    def test_row_count_mismatch_writes_nothing(self):
        path = self.write(row())
        with self.assertRaises(aggregation.AggregationError):
            aggregation.aggregate_manifest_entry(path.parent, self.entry(path, 99))
        self.assertFalse(ArchiveFile.objects.exists())

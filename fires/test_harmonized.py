import json
from datetime import date

import numpy as np
from django.core.cache import caches
from django.test import SimpleTestCase, TestCase

from fires import harmonized
from fires.geography import point_in_geometry
from fires.models import ArchiveCellDay, ArchiveFile

BOX = {'south': 9.0, 'west': 5.0, 'north': 10.0, 'east': 6.0}


class PureFunctionTests(SimpleTestCase):
    def test_percentile_rank_uses_mid_rank_for_ties(self):
        self.assertEqual(harmonized.percentile_rank(0, np.zeros(10)), 50)
        self.assertEqual(harmonized.percentile_rank(5, np.array([1.0, 2.0, 5.0, 9.0])), 62.5)

    def test_categories(self):
        self.assertEqual(
            [harmonized.categorize(value) for value in (0, 19.9, 20, 79.9, 80, 94.9, 95, 100)],
            ['below_normal', 'below_normal', 'normal', 'normal', 'above_normal', 'above_normal', 'extreme', 'extreme'],
        )

    def test_a_date_keeps_its_slot_across_leap_years(self):
        self.assertEqual(harmonized.day_slot(date(2001, 3, 1)), harmonized.day_slot(date(2004, 3, 1)))
        self.assertEqual(harmonized.day_slot(date(2004, 2, 29)), 59)

    def test_families_are_averaged_so_two_viirs_do_not_outvote_modis(self):
        row = harmonized._combine('2020-01', {
            'MODIS_AQUA': (5, 20.0, 10), 'VIIRS_SNPP': (50, 80.0, 10), 'VIIRS_NOAA20': (50, 100.0, 10),
        })
        self.assertEqual((row['modis_index'], row['viirs_index'], row['index']), (20.0, 90.0, 55.0))

    def test_no_detections_is_its_own_category(self):
        self.assertEqual(harmonized._combine('2020-01', {'MODIS_AQUA': (0, 50.0, 10)})['category'], 'no_detections')


class PolygonTests(SimpleTestCase):
    def test_matches_the_shared_geometry_check_with_holes_and_multiple_parts(self):
        geometry = {'type': 'MultiPolygon', 'coordinates': [
            [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]], [[4, 4], [6, 4], [6, 6], [4, 6], [4, 4]]],
            [[[20, 0], [24, 6], [28, 0], [20, 0]]],
        ]}
        prepared = harmonized._prepare_polygons(geometry)
        for lon in np.arange(-1.3, 30, 0.7):
            for lat in np.arange(-1.1, 12, 0.7):
                self.assertEqual(
                    harmonized.polygons_contain(float(lon), float(lat), prepared),
                    point_in_geometry(float(lon), float(lat), geometry), (lon, lat),
                )


class ArchiveTestCase(TestCase):
    def setUp(self):
        caches['harmonized'].clear()

    def add_year(self, year, platform, monthly, first=None, last=None, complete=True, product=None):
        """monthly maps month -> detections, stored on the 15th."""
        first = first or date(year, 1, 1)
        last = last or date(year, 12, 31)
        source = ArchiveFile.objects.create(
            country='Nigeria', product=product or platform, year=year, sha256='x', aggregation_version=1,
            grid_degrees=0.25, rows_read=1, vegetation_rows=1, rejected_rows=0,
            platform_stats={platform: {
                'first_date': first.isoformat(), 'last_date': last.isoformat(),
                'detections': sum(monthly.values()), 'days_with_detections': len(monthly), 'complete_year': complete,
            }},
        )
        ArchiveCellDay.objects.bulk_create(
            ArchiveCellDay(
                source_file=source, platform=platform, acq_date=date(year, month, 15), cell_lat=37, cell_lon=21,
                detections=count, daytime=count, nighttime=0, low_confidence=0, frp_sum=1.0,
            )
            for month, count in monthly.items()
        )

    def series(self):
        return harmonized.load_series(frozenset({(37, 21)}))

    def row(self, rows, period):
        return next((item for item in rows if item['period'] == period), None)


class IndexTests(ArchiveTestCase):
    def fill(self, platform='MODIS_AQUA', years=range(2003, 2011)):
        for offset, year in enumerate(years):
            self.add_year(year, platform, {1: 10 + offset})

    def test_highest_month_on_record_is_ranked_at_the_top(self):
        self.fill()
        row = self.row(harmonized.monthly_index(self.series()), '2010-01')
        self.assertEqual(row['platforms']['MODIS_AQUA']['percentile'], 100)
        self.assertEqual(row['platforms']['MODIS_AQUA']['reference_values'], 7)  # Own year left out.
        self.assertEqual(row['category'], 'extreme')

    def test_different_sensor_scales_give_the_same_index(self):
        self.fill('MODIS_AQUA')
        for offset, year in enumerate(range(2003, 2011)):
            self.add_year(year, 'VIIRS_SNPP', {1: (10 + offset) * 8})
        row = self.row(harmonized.monthly_index(self.series()), '2008-01')
        self.assertEqual(row['modis_index'], row['viirs_index'])

    def test_partial_year_is_excluded_from_reference_and_partial_month_is_not_scored(self):
        self.fill()
        self.add_year(2002, 'MODIS_AQUA', {7: 500}, first=date(2002, 7, 4), complete=False)
        series = self.series()
        self.assertNotIn(2002, series['MODIS_AQUA'].reference_years)
        rows = harmonized.monthly_index(series)
        self.assertIsNone(self.row(rows, '2002-07'))

    def test_snpp_2012_is_not_a_reference_year(self):
        self.fill('VIIRS_SNPP', range(2012, 2020))
        self.assertNotIn(2012, self.series()['VIIRS_SNPP'].reference_years)

    def test_terra_is_not_used_after_its_cutoff(self):
        self.fill('MODIS_TERRA', range(2015, 2025))
        series = self.series()
        self.assertNotIn(2023, series['MODIS_TERRA'].reference_years)
        rows = harmonized.monthly_index(series)
        self.assertIsNone(self.row(rows, '2023-01'))
        self.assertIsNotNone(self.row(rows, '2022-01'))

    def test_too_few_reference_years_gives_no_score(self):
        self.fill(years=range(2003, 2007))
        self.assertEqual(harmonized.monthly_index(self.series()), [])

    def test_days_without_records_count_as_zero_inside_the_operating_span(self):
        self.fill()
        rows = harmonized.daily_index(self.series(), date(2010, 1, 15), date(2010, 1, 16))
        self.assertEqual(self.row(rows, '2010-01-15')['platforms']['MODIS_AQUA']['detections'], 19 - 2)
        self.assertEqual(self.row(rows, '2010-01-16')['category'], 'no_detections')

    def test_days_outside_the_operating_span_are_not_scored(self):
        self.fill()
        self.assertEqual(harmonized.daily_index(self.series(), date(2011, 1, 15), date(2011, 1, 15)), [])

    def test_leap_day_is_handled(self):
        self.fill(years=range(2003, 2011))
        rows = harmonized.monthly_index(self.series())
        self.assertIsNotNone(self.row(rows, '2004-02'))
        self.assertIsNotNone(self.row(rows, '2005-02'))


class AreaTests(ArchiveTestCase):
    def test_global_scope_is_unavailable(self):
        self.assertEqual(harmonized.resolve_area(None, None)['status'], 'unavailable')

    def test_area_outside_archive_is_unavailable(self):
        self.add_year(2005, 'MODIS_AQUA', {1: 5})
        area = harmonized.resolve_area({'south': -10.0, 'west': -60.0, 'north': -9.0, 'east': -59.0}, None)
        self.assertEqual(area['status'], 'unavailable')

    def test_fully_covered_area_is_complete(self):
        self.add_year(2005, 'MODIS_AQUA', {1: 5})
        area = harmonized.resolve_area({'south': 9.25, 'west': 5.25, 'north': 9.49, 'east': 5.49}, None)
        self.assertEqual(area['status'], 'complete')
        self.assertEqual(area['cells'], frozenset({(37, 21)}))


class EndpointTests(ArchiveTestCase):
    def params(self, **extra):
        return {'geo_scope': 'bbox', 'geo_scope_identifier': json.dumps({'south': 9.25, 'west': 5.25, 'north': 9.49, 'east': 5.49}), **extra}

    def fill(self):
        for offset, year in enumerate(range(2003, 2011)):
            self.add_year(year, 'MODIS_AQUA', {1: 10 + offset})

    def test_monthly_endpoint(self):
        self.fill()
        body = self.client.get('/api/harmonized/monthly/', self.params(start_year=2010, end_year=2010)).json()
        self.assertEqual(body['coverage']['status'], 'complete')
        self.assertEqual(body['rows'][0]['period'], '2010-01')
        self.assertIn('MODIS_AQUA', body['metadata']['reference_years'])

    def test_daily_endpoint_requires_a_bounded_range(self):
        self.fill()
        self.assertEqual(self.client.get('/api/harmonized/daily/', self.params()).status_code, 400)
        long_range = self.params(start_date='2005-01-01', end_date='2007-01-01')
        self.assertEqual(self.client.get('/api/harmonized/daily/', long_range).status_code, 400)
        ok = self.params(start_date='2010-01-14', end_date='2010-01-16')
        self.assertEqual(len(self.client.get('/api/harmonized/daily/', ok).json()['rows']), 3)

    def test_uncovered_scope_returns_no_rows(self):
        body = self.client.get('/api/harmonized/monthly/', {'geo_scope': 'global'}).json()
        self.assertEqual(body['coverage']['status'], 'unavailable')
        self.assertEqual(body['rows'], [])

    def test_bad_year_is_rejected(self):
        self.assertEqual(self.client.get('/api/harmonized/monthly/', self.params(start_year='abc')).status_code, 400)

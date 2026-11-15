import csv
import importlib
import os
import threading
import uuid
import tempfile
from types import SimpleNamespace
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import requests
from django.core.management import call_command
from django.apps import apps
from django.db import connections
from django.db.backends.sqlite3.base import DatabaseWrapper
from django.test import TestCase, override_settings
from django.utils import timezone
from django.urls import reverse
from rest_framework.test import APITransactionTestCase, APITestCase

from fires.management.commands.import_fire_data import _build_record_key
from fires.geography import point_in_geometry, resolve_region, resolve_country, list_regions
from fires.models import CoverageLedger, FireDetection, HistoricalJob, HistoricalJobDetection, LiveRetrievalRun
from fires.services import HistoricalJobService, _FIRMS_DATASETS
from fires.live import LIVE_PRODUCTS, LiveRetrievalService, _live_database_process_lock, _persist_live_detection_batch
from analysis.fetch_data import build_firms_url, date_chunks, fetch_dataset_chunk, fetch_availability_ranges


class FireScopeAPITests(APITestCase):
    def test_health_endpoint(self):
        response = self.client.get(reverse('health'))
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.data['ok'])

    def test_summary_endpoint(self):
        response = self.client.get(reverse('summary'))
        self.assertEqual(response.status_code, 200)
        self.assertIn('generated_at', response.data)
        self.assertIn('sensors', response.data)

    def test_detections_endpoint_paginates(self):
        response = self.client.get(reverse('detections'), {'page_size': 5})
        self.assertEqual(response.status_code, 200)
        self.assertIn('results', response.data)
        self.assertLessEqual(len(response.data['results']), 5)

    def test_live_scope_excludes_historical_rows_and_requires_retrieval_provenance(self):
        today = timezone.now().date()
        historical = FireDetection.objects.create(
            source_product='MODIS_SP', sensor='MODIS', latitude=1, longitude=2,
            acq_date=today, record_key='scope-historical',
        )
        live = FireDetection.objects.create(
            source_product='MODIS_NRT', sensor='MODIS', latitude=1, longitude=2,
            acq_date=today, record_key='scope-live', retrieved_at=timezone.now(),
        )
        response = self.client.get(reverse('detections'), {'scope': 'live'})
        self.assertEqual(response.data['count'], 1)
        self.assertEqual(response.data['results'][0]['id'], live.id)
        response = self.client.get(reverse('detections'), {'scope': 'historical'})
        self.assertEqual(response.data['count'], 1)
        self.assertEqual(response.data['results'][0]['id'], historical.id)

    def test_map_points_requires_bounds_and_limits_results(self):
        response = self.client.get(reverse('map_points'))
        self.assertEqual(response.status_code, 400)

    def test_map_points_rejects_wrapped_or_non_finite_longitudes(self):
        for west, east in [('-191.95', '-180'), ('-10', 'NaN'), ('10', '-10')]:
            with self.subTest(west=west, east=east):
                response = self.client.get(reverse('map_points'), {
                    'south': '-5', 'west': west, 'north': '5', 'east': east,
                })
                self.assertEqual(response.status_code, 400)

    def test_abia_resolves_using_nigeria_iso3_and_canonical_adm1(self):
        details = resolve_region({'country': 'NGA', 'type': 'ADM1', 'name': 'Abia'})[0]
        self.assertEqual(details['country_code'], 'NGA')
        self.assertEqual(details['administrative_level'], 'ADM1')
        self.assertEqual(details['type'], 'state')
        self.assertEqual(details['name'], 'Abia')
        self.assertLess(details['bbox']['west'], details['bbox']['east'])

    def test_map_points_filters_to_viewport_and_scope(self):
        today = timezone.now().date()
        FireDetection.objects.create(source_product='MODIS_NRT', sensor='MODIS', latitude=1, longitude=2,
                                     acq_date=today, record_key='map-inside', retrieved_at=timezone.now())
        FireDetection.objects.create(source_product='MODIS_NRT', sensor='MODIS', latitude=50, longitude=50,
                                     acq_date=today, record_key='map-outside', retrieved_at=timezone.now())
        response = self.client.get(reverse('map_points'), {
            'south': '-5', 'west': '-5', 'north': '5', 'east': '5', 'scope': 'live', 'zoom': 10,
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['count'], 1)
        self.assertEqual(response.data['results'][0]['latitude'], 1)

    def test_map_points_clusters_all_matches_by_sensor_and_product_at_broad_zoom(self):
        today = timezone.now().date()
        for index in range(12):
            FireDetection.objects.create(source_product='MODIS_NRT', sensor='MODIS', latitude=1 + index / 1000,
                                         longitude=2 + index / 1000, acq_date=today,
                                         record_key=f'map-cluster-modis-{index}', retrieved_at=timezone.now())
        FireDetection.objects.create(source_product='VIIRS_SNPP_NRT', sensor='VIIRS_SNPP', latitude=1.002,
                                     longitude=2.002, acq_date=today, record_key='map-cluster-viirs',
                                     retrieved_at=timezone.now())
        response = self.client.get(reverse('map_points'), {
            'south': '-5', 'west': '-5', 'north': '5', 'east': '5', 'scope': 'live', 'zoom': 2,
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['mode'], 'clusters')
        self.assertEqual(response.data['count'], 13)
        self.assertEqual(sum(cluster['count'] for cluster in response.data['clusters']), 13)
        self.assertEqual({cluster['source_product'] for cluster in response.data['clusters']},
                         {'MODIS_NRT', 'VIIRS_SNPP_NRT'})

    def test_map_points_returns_actual_observations_when_zoomed_in(self):
        today = timezone.now().date()
        detection = FireDetection.objects.create(source_product='MODIS_NRT', sensor='MODIS', latitude=1, longitude=2,
                                                  acq_date=today, record_key='map-zoomed-point', frp=4.5)
        response = self.client.get(reverse('map_points'), {
            'south': '0', 'west': '1', 'north': '2', 'east': '3', 'zoom': 10,
        })
        self.assertEqual(response.data['mode'], 'points')
        self.assertEqual(response.data['count'], 1)
        self.assertEqual(response.data['results'][0]['id'], detection.id)

    def test_dense_close_view_clusters_instead_of_dropping_past_point_limit(self):
        today = timezone.now().date()
        FireDetection.objects.bulk_create([
            FireDetection(source_product='MODIS_NRT', sensor='MODIS', latitude=1 + index / 10000,
                          longitude=2, acq_date=today, record_key=f'map-dense-{index}',
                          retrieved_at=timezone.now())
            for index in range(1510)
        ])
        response = self.client.get(reverse('map_points'), {
            'south': '0', 'west': '1', 'north': '2', 'east': '3', 'zoom': 12, 'limit': 1500,
        })
        self.assertEqual(response.data['mode'], 'clusters')
        self.assertEqual(response.data['count'], 1510)
        self.assertEqual(sum(cluster['count'] for cluster in response.data['clusters']), 1510)

    def test_map_points_intersects_viewport_with_geographic_bbox(self):
        today = timezone.now().date()
        FireDetection.objects.create(source_product='MODIS_NRT', sensor='MODIS', latitude=1, longitude=2,
                                     acq_date=today, record_key='map-bbox-inside', retrieved_at=timezone.now())
        FireDetection.objects.create(source_product='MODIS_NRT', sensor='MODIS', latitude=4, longitude=2,
                                     acq_date=today, record_key='map-bbox-outside', retrieved_at=timezone.now())
        response = self.client.get(reverse('map_points'), {
            'south': '-5', 'west': '-5', 'north': '5', 'east': '5',
            'geo_scope': 'bbox', 'geo_scope_identifier': '{"south":0,"west":0,"north":3,"east":3}',
            'zoom': 10,
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['count'], 1)
        self.assertEqual(response.data['results'][0]['latitude'], 1)

    @patch('fires.views.get_region_details')
    def test_map_points_filters_region_polygon_after_bbox(self, get_region_details_mock):
        today = timezone.now().date()
        FireDetection.objects.create(source_product='MODIS_NRT', sensor='MODIS', latitude=0.5, longitude=0.5,
                                     acq_date=today, record_key='map-region-inside', retrieved_at=timezone.now())
        FireDetection.objects.create(source_product='MODIS_NRT', sensor='MODIS', latitude=1.8, longitude=0.4,
                                     acq_date=today, record_key='map-region-outside-polygon', retrieved_at=timezone.now())
        get_region_details_mock.return_value = {
            'boundary_code': 'test-id', 'bbox': {'south': 0, 'west': 0, 'north': 2, 'east': 2},
            'polygon': {'type': 'Polygon', 'coordinates': [[[0, 0], [2, 0], [0, 2], [0, 0]]]},
        }
        response = self.client.get(reverse('map_points'), {
            'south': '-1', 'west': '-1', 'north': '3', 'east': '3',
            'geo_scope': 'region', 'geo_scope_identifier': 'USA:ADM1:test-id:Test Place', 'zoom': 15,
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['count'], 1)
        self.assertEqual(response.data['results'][0]['latitude'], 0.5)

    def test_charts_endpoint_lists_files(self):
        response = self.client.get(reverse('charts'))
        self.assertEqual(response.status_code, 200)
        self.assertIn('results', response.data)

    def test_coverage_endpoint_filters_paginates_and_leaves_absent_dates_unknown(self):
        base = date(2026, 10, 10)
        for offset in range(3):
            CoverageLedger.objects.create(
                source_product='MODIS_NRT', acquisition_date=base + timedelta(days=offset),
                scope_type='global', scope_identifier='world', availability_outcome='available',
                retrieval_outcome='succeeded', observation_outcome='zero_detections',
                attempt_key=f'api-coverage-{offset}', evidence={},
            )
        response = self.client.get(reverse('coverage_records'), {
            'source_product': 'modis_nrt', 'start_date': base.isoformat(),
            'end_date': (base + timedelta(days=2)).isoformat(), 'scope': 'global', 'page_size': 1,
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['count'], 3)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['retrieval_outcome'], 'succeeded')
        self.assertEqual(response.data['results'][0]['observation_outcome'], 'zero_detections')
        unknown = self.client.get(reverse('coverage_records'), {
            'source_product': 'MODIS_NRT', 'start_date': '2026-10-20', 'end_date': '2026-10-20',
        })
        self.assertEqual(unknown.data['count'], 0)

    def _aggregate_detection(self, product, day, key, frp=10.0, daynight='D', retrieved=True):
        sensor = product.replace('_NRT', '').replace('_SP', '')
        return FireDetection.objects.create(
            source_product=product, sensor=sensor, latitude=1, longitude=2,
            acq_date=day, frp=frp, daynight=daynight,
            record_key=key, retrieved_at=timezone.now() if retrieved else None,
        )

    def _coverage(self, product, day, key, *, retrieval='succeeded', observation='detections_returned',
                  scope_type='global', scope_identifier='world', job=None, detection_count=1, rejected_count=0):
        return CoverageLedger.objects.create(
            source_product=product, acquisition_date=day, scope_type=scope_type,
            scope_identifier=scope_identifier, availability_outcome='available',
            retrieval_outcome=retrieval, observation_outcome=observation,
            detection_count=detection_count, rejected_count=rejected_count,
            attempt_key=key, historical_job=job,
        )

    @override_settings(DEBUG=True)
    def test_daily_aggregation_keeps_all_eight_products_distinct(self):
        day = date(2026, 9, 1)
        products = (
            'MODIS_NRT', 'MODIS_SP', 'VIIRS_SNPP_NRT', 'VIIRS_SNPP_SP',
            'VIIRS_NOAA20_NRT', 'VIIRS_NOAA20_SP', 'VIIRS_NOAA21_NRT', 'VIIRS_NOAA21_SP',
        )
        for index, product in enumerate(products):
            self._aggregate_detection(product, day, f'daily-product-{index}')
        response = self.client.get(reverse('daily_aggregation'), {
            'start_date': day.isoformat(), 'end_date': day.isoformat(), 'page_size': 100,
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['count'], 8)
        self.assertEqual({row['source_product'] for row in response.data['results']}, set(products))
        by_sensor = self.client.get(reverse('daily_aggregation'), {
            'sensor': 'MODIS', 'start_date': day.isoformat(), 'end_date': day.isoformat(),
            'page_size': 100,
        })
        self.assertEqual(by_sensor.data['count'], 2)
        self.assertEqual({row['source_product'] for row in by_sensor.data['results']}, {'MODIS_NRT', 'MODIS_SP'})

    def test_region_daily_and_monthly_aggregation_preserves_coverage_states(self):
        first = date(2026, 9, 20)
        scope_id = 'NGA:state:NG-KN:Kano'
        job = HistoricalJob.objects.create(
            status='completed', stage='completed', request_hash='regional-coverage-states',
            start_date=first, end_date=first + timedelta(days=3),
            request_params={'region': {'country_code': 'NGA', 'type': 'state', 'boundary_code': 'NG-KN', 'name': 'Kano'}},
        )
        detection = self._aggregate_detection('MODIS_SP', first, 'regional-state-detection', 18, retrieved=False)
        HistoricalJobDetection.objects.create(job=job, detection=detection)
        self._coverage('MODIS_SP', first, 'regional-detections', scope_type='region', scope_identifier=scope_id, job=job)
        self._coverage('MODIS_SP', first + timedelta(days=1), 'regional-zero', scope_type='region', scope_identifier=scope_id, job=job, observation='zero_detections', detection_count=0)
        self._coverage('MODIS_SP', first + timedelta(days=2), 'regional-failure', scope_type='region', scope_identifier=scope_id, job=job, retrieval='failed', observation='unknown', detection_count=None)

        daily = self.client.get(reverse('daily_aggregation'), {
            'source_product': 'MODIS_SP', 'start_date': first.isoformat(),
            'end_date': (first + timedelta(days=3)).isoformat(), 'scope': 'region',
            'scope_identifier': scope_id, 'page_size': 100,
        })
        self.assertEqual(daily.status_code, 200)
        rows = {row['acquisition_date']: row for row in daily.data['results']}
        self.assertEqual([rows[(first + timedelta(days=i)).isoformat()]['coverage_state'] for i in range(4)], [
            'detections_returned', 'confirmed_zero_detections', 'failed', 'unknown',
        ])
        self.assertEqual(rows[(first + timedelta(days=1)).isoformat()]['detection_count'], 0)
        self.assertIsNone(rows[(first + timedelta(days=2)).isoformat()]['detection_count'])
        self.assertIsNone(rows[(first + timedelta(days=3)).isoformat()]['detection_count'])

        monthly = self.client.get(reverse('monthly_aggregation'), {
            'source_product': 'MODIS_SP', 'start_date': first.isoformat(),
            'end_date': (first + timedelta(days=3)).isoformat(), 'scope': 'region',
            'scope_identifier': scope_id,
        })
        self.assertEqual(monthly.status_code, 200)
        result = monthly.data['results'][0]
        self.assertEqual(result['scope_identifier'], scope_id)
        self.assertEqual(result['total_detections'], 1)
        self.assertFalse(result['total_detections_complete_for_scope'])
        self.assertEqual(result['confirmed_zero_dates'], 1)
        self.assertEqual(result['failed_dates'], 1)
        self.assertEqual(result['unknown_dates'], 1)

    def test_area_scope_counts_worldwide_detections_inside_it(self):
        day = date(2026, 9, 10)
        self._aggregate_detection('VIIRS_SNPP_NRT', day, 'inside-area')  # lat 1, lon 2
        outside = self._aggregate_detection('VIIRS_SNPP_NRT', day, 'outside-area')
        FireDetection.objects.filter(pk=outside.pk).update(latitude=40, longitude=40)
        self._coverage('VIIRS_SNPP_NRT', day, 'world-day', detection_count=2)
        self._coverage('VIIRS_SNPP_NRT', day + timedelta(days=1), 'world-zero',
                       observation='zero_detections', detection_count=0)
        bbox_id = '{"east":3.0,"north":2.0,"south":-2.0,"west":-3.0}'
        response = self.client.get(reverse('daily_aggregation'), {
            'source_product': 'VIIRS_SNPP_NRT', 'start_date': day.isoformat(),
            'end_date': (day + timedelta(days=2)).isoformat(), 'scope': 'bbox',
            'scope_identifier': bbox_id, 'page_size': 100,
        })
        self.assertEqual(response.status_code, 200)
        rows = [(r['detection_count'], r['coverage_state']) for r in response.data['results']]
        self.assertEqual(rows, [
            (1, 'detections_returned'), (0, 'confirmed_zero_detections'), (None, 'unknown'),
        ])

    def test_bbox_identifier_is_canonicalized_to_persisted_historical_scope(self):
        day = date(2026, 9, 24)
        bbox_id = '{"east":3.0,"north":2.0,"south":-2.0,"west":-3.0}'
        job = HistoricalJob.objects.create(
            status='completed', stage='completed', request_hash='bbox-scope-aggregation',
            start_date=day, end_date=day,
            request_params={'bbox': {'south': -2.0, 'west': -3.0, 'north': 2.0, 'east': 3.0}},
        )
        detection = self._aggregate_detection('VIIRS_SNPP_SP', day, 'bbox-scoped-detection', 12, retrieved=False)
        HistoricalJobDetection.objects.create(job=job, detection=detection)
        self._coverage('VIIRS_SNPP_SP', day, 'bbox-scoped-coverage', scope_type='bbox', scope_identifier=bbox_id, job=job)
        response = self.client.get(reverse('daily_aggregation'), {
            'source_product': 'VIIRS_SNPP_SP', 'start_date': day.isoformat(), 'end_date': day.isoformat(),
            'scope': 'bbox', 'scope_identifier': '{"south":-2,"west":-3,"north":2,"east":3}',
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['results'][0]['scope_identifier'], bbox_id)
        self.assertEqual(response.data['results'][0]['detection_count'], 1)

    def test_invalid_bbox_scope_is_rejected(self):
        response = self.client.get(reverse('daily_aggregation'), {
            'source_product': 'MODIS_NRT', 'start_date': '2026-09-01', 'end_date': '2026-09-01',
            'scope': 'bbox', 'scope_identifier': '{"south":10,"west":2,"north":0,"east":3}',
        })
        self.assertEqual(response.status_code, 400)

    def test_daily_aggregation_frp_day_night_missing_and_unknown(self):
        day = date(2026, 9, 2)
        self._aggregate_detection('MODIS_NRT', day, 'daily-frp-1', 10, 'D')
        self._aggregate_detection('MODIS_NRT', day, 'daily-frp-2', 30, 'N')
        self._aggregate_detection('MODIS_NRT', day, 'daily-frp-3', None, 'D')
        self._coverage('MODIS_NRT', day, 'daily-frp-ledger', detection_count=3)
        response = self.client.get(reverse('daily_aggregation'), {
            'source_product': 'MODIS_NRT', 'start_date': day.isoformat(), 'end_date': day.isoformat(),
        })
        row = response.data['results'][0]
        self.assertEqual(row['detection_count'], 3)
        self.assertEqual(row['mean_frp'], 20)
        self.assertEqual(row['median_frp'], 20)
        self.assertEqual(row['maximum_frp'], 30)
        self.assertEqual(row['daytime_detection_count'], 2)
        self.assertEqual(row['nighttime_detection_count'], 1)
        self.assertEqual(row['missing_frp_count'], 1)

        unknown_day = date(2026, 9, 3)
        unknown = self.client.get(reverse('daily_aggregation'), {
            'source_product': 'MODIS_NRT', 'start_date': unknown_day.isoformat(),
            'end_date': unknown_day.isoformat(),
        }).data['results'][0]
        self.assertEqual(unknown['coverage_state'], 'unknown')
        self.assertIsNone(unknown['detection_count'])

    def test_daily_coverage_states_retries_and_scope_are_separate(self):
        day = date(2026, 9, 4)
        self._coverage('MODIS_NRT', day, 'coverage-failed', retrieval='failed', observation='unknown', detection_count=None)
        self._coverage('MODIS_NRT', day, 'coverage-retry', observation='detections_returned')
        self._aggregate_detection('MODIS_NRT', day, 'coverage-positive-row')
        response = self.client.get(reverse('daily_aggregation'), {
            'source_product': 'MODIS_NRT', 'start_date': day.isoformat(), 'end_date': day.isoformat(),
        })
        row = response.data['results'][0]
        self.assertEqual(row['coverage_state'], 'detections_returned')
        self.assertEqual(row['retrieval_evidence']['attempt_count'], 2)
        self.assertEqual(row['detection_count'], 1)
        self.assertEqual(row['retrieval_evidence']['retrieval_outcome_counts']['failed'], 1)

        regional_job = HistoricalJob.objects.create(
            status='completed', stage='completed', request_hash='regional-aggregation',
            start_date=day, end_date=day, request_params={'region': {'name': 'Sample region'}},
        )
        regional_detection = self._aggregate_detection(
            'MODIS_SP', day, 'regional-observation', 22, retrieved=False,
        )
        HistoricalJobDetection.objects.create(job=regional_job, detection=regional_detection)
        self._coverage(
            'MODIS_SP', day, 'regional-coverage', scope_type='region',
            scope_identifier='TEST:state:X:Sample region', job=regional_job,
        )
        global_row = self.client.get(reverse('daily_aggregation'), {
            'source_product': 'MODIS_SP', 'start_date': day.isoformat(), 'end_date': day.isoformat(),
        }).data['results'][0]
        self.assertEqual(global_row['coverage_state'], 'unknown')
        regional_row = self.client.get(reverse('daily_aggregation'), {
            'source_product': 'MODIS_SP', 'start_date': day.isoformat(), 'end_date': day.isoformat(),
            'scope': 'region', 'scope_identifier': 'TEST:state:X:Sample region',
        }).data['results'][0]
        self.assertEqual(regional_row['scope_type'], 'region')
        self.assertEqual(regional_row['detection_count'], 1)
        self.assertEqual(regional_row['coverage_state'], 'detections_returned')

    def test_daily_unavailable_partial_and_failure_states_and_pagination(self):
        base = date(2026, 9, 10)
        self._coverage('MODIS_NRT', base, 'state-zero', observation='zero_detections', detection_count=0)
        self._coverage('MODIS_NRT', base + timedelta(days=1), 'state-unavailable',
                       retrieval='unavailable', observation='unknown', detection_count=None)
        self._coverage('MODIS_NRT', base + timedelta(days=2), 'state-failed',
                       retrieval='failed', observation='unknown', detection_count=None)
        self._coverage('MODIS_NRT', base + timedelta(days=3), 'state-partial',
                       retrieval='partial', observation='unknown', detection_count=None, rejected_count=2)
        response = self.client.get(reverse('daily_aggregation'), {
            'source_product': 'MODIS_NRT', 'start_date': base.isoformat(),
            'end_date': (base + timedelta(days=3)).isoformat(), 'page_size': 2,
        })
        self.assertEqual(response.data['count'], 4)
        self.assertEqual(len(response.data['results']), 2)
        states = self.client.get(reverse('daily_aggregation'), {
            'source_product': 'MODIS_NRT', 'start_date': base.isoformat(),
            'end_date': (base + timedelta(days=3)).isoformat(), 'page_size': 100,
        }).data['results']
        self.assertEqual([row['coverage_state'] for row in states], [
            'confirmed_zero_detections', 'unavailable', 'failed', 'incomplete',
        ])

    def test_monthly_aggregation_uses_eligible_dates_and_coverage_denominator(self):
        first, second, zero = date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3)
        self._aggregate_detection('MODIS_NRT', first, 'monthly-1', 10, 'D')
        self._aggregate_detection('MODIS_NRT', first, 'monthly-2', 20, 'D')
        self._aggregate_detection('MODIS_NRT', second, 'monthly-3', 40, 'N')
        self._coverage('MODIS_NRT', first, 'monthly-ledger-1', detection_count=2)
        self._coverage('MODIS_NRT', first, 'monthly-failed-retry', retrieval='failed', observation='unknown', detection_count=None)
        self._coverage('MODIS_NRT', second, 'monthly-ledger-2', detection_count=1)
        self._coverage('MODIS_NRT', zero, 'monthly-ledger-zero', observation='zero_detections', detection_count=0)
        self._coverage('MODIS_NRT', date(2026, 9, 4), 'monthly-unavailable',
                       retrieval='unavailable', observation='unknown', detection_count=None)
        self._coverage('MODIS_NRT', date(2026, 9, 5), 'monthly-failed',
                       retrieval='failed', observation='unknown', detection_count=None)
        self._coverage('MODIS_NRT', date(2026, 9, 6), 'monthly-incomplete',
                       retrieval='partial', observation='unknown', detection_count=None, rejected_count=1)
        last = date(2026, 9, 7)
        response = self.client.get(reverse('monthly_aggregation'), {
            'source_product': 'MODIS_NRT', 'start_date': first.isoformat(),
            'end_date': last.isoformat(),
        })
        self.assertEqual(response.status_code, 200)
        row = response.data['results'][0]
        self.assertEqual(row['total_detections'], 3)
        self.assertEqual(row['daily_mean_detection_count'], 1)
        self.assertEqual(row['eligible_observed_dates'], 3)
        self.assertEqual(row['mean_frp'], 70 / 3)
        self.assertEqual(row['median_frp'], 20)
        self.assertEqual(row['maximum_frp'], 40)
        self.assertEqual(row['daytime_detection_count'], 2)
        self.assertEqual(row['nighttime_detection_count'], 1)
        self.assertEqual(row['dates_with_detections'], 2)
        self.assertEqual(row['confirmed_zero_dates'], 1)
        self.assertEqual(row['unavailable_dates'], 1)
        self.assertEqual(row['failed_dates'], 1)
        self.assertEqual(row['incomplete_dates'], 1)
        self.assertEqual(row['unknown_dates'], 1)
        self.assertEqual(row['dates_with_failed_attempts'], 2)
        self.assertEqual(row['coverage_denominator_days'], 7)
        self.assertFalse(row['total_detections_complete_for_scope'])

    def test_aggregation_filters_empty_results_and_date_limits(self):
        response = self.client.get(reverse('daily_aggregation'), {
            'source_product': 'VIIRS_SNPP_SP', 'sensor': 'VIIRS_NOAA20',
            'start_date': '2026-09-01', 'end_date': '2026-09-01',
        })
        self.assertEqual(response.status_code, 400)
        empty = self.client.get(reverse('monthly_aggregation'), {
            'source_product': 'VIIRS_SNPP_SP', 'start_date': '2026-09-01',
            'end_date': '2026-09-01',
        })
        self.assertEqual(empty.status_code, 200)
        self.assertIsNone(empty.data['results'][0]['total_detections'])
        self.assertEqual(empty.data['results'][0]['unknown_dates'], 1)
        too_wide = self.client.get(reverse('daily_aggregation'), {
            'start_date': '2020-01-01', 'end_date': '2026-01-01',
        })
        self.assertEqual(too_wide.status_code, 400)


class HistoricalGeographyTests(TestCase):
    def test_invalid_nigeria_administrative_region_is_rejected(self):
        response = self.client.get(reverse('region_detail'), {
            'country': 'Nigeria', 'type': 'state', 'name': 'Not a real Nigerian state',
        })
        self.assertEqual(response.status_code, 400)
        self.assertIn('Unknown Nigeria state', response.data['detail'])

    def test_coverage_backfill_uses_historical_payload_without_inventing_zeroes(self):
        day = date(2026, 1, 2)
        job = HistoricalJob.objects.create(
            status='completed', stage='completed', request_hash='coverage-backfill',
            start_date=day, end_date=day,
            result_payload={'datasets': [{
                'dataset': 'MODIS_SP', 'covered_dates': [day.isoformat()], 'available': True,
            }]},
        )
        migration = importlib.import_module('fires.migrations.0006_coverage_ledger')
        migration._backfill_coverage(apps, SimpleNamespace(connection=connections['default']))
        entry = CoverageLedger.objects.get(historical_job=job, acquisition_date=day)
        self.assertEqual(entry.retrieval_outcome, 'succeeded')
        self.assertEqual(entry.observation_outcome, 'unknown')
        self.assertIsNone(entry.detection_count)

    def test_historical_bbox_attempt_keeps_regional_scope_and_zero_result(self):
        day = date(2026, 10, 19)
        job = HistoricalJob.objects.create(
            status='fetching', stage='fetching', request_hash='bbox-coverage',
            start_date=day, end_date=day, products=['MODIS'],
            request_params={'bbox': {'south': -2, 'west': -3, 'north': 2, 'east': 3}},
        )
        with patch.dict(os.environ, {'FIRMS_MAP_KEY': 'test-map-key'}), \
             patch('fires.services.fetch_availability_ranges', return_value=[(day, day)]), \
             patch('fires.services.fetch_dataset_chunk', return_value=pd.DataFrame()):
            HistoricalJobService._fetch_and_persist(job)
        entry = CoverageLedger.objects.get(historical_job=job, acquisition_date=day)
        self.assertEqual(entry.scope_type, 'bbox')
        self.assertEqual(entry.retrieval_outcome, 'succeeded')
        self.assertEqual(entry.observation_outcome, 'zero_detections')

    def test_sensor_job_falls_back_to_nrt_for_dates_sp_has_not_published(self):
        sp_day, nrt_day = date(2026, 8, 30), date(2026, 9, 2)
        job = HistoricalJob.objects.create(
            status='fetching', stage='fetching', request_hash='nrt-fallback',
            start_date=sp_day, end_date=nrt_day, sensors=['MODIS'], request_params={'bbox': {}},
        )
        ranges = {'MODIS_SP': [(date(2000, 1, 1), sp_day)], 'MODIS_NRT': [(date(2026, 8, 1), nrt_day)]}
        fetched = []
        def fetch(dataset, start, day_range, bbox, key):
            fetched.append((dataset, start, day_range))
            return pd.DataFrame()
        with patch.dict(os.environ, {'FIRMS_MAP_KEY': 'test-map-key'}),              patch('fires.services.fetch_availability_ranges', side_effect=lambda ds, key: ranges[ds]),              patch('fires.services.fetch_dataset_chunk', side_effect=fetch):
            result = HistoricalJobService._fetch_and_persist(job)
        self.assertEqual({ds for ds, _, _ in fetched}, {'MODIS_SP', 'MODIS_NRT'})
        self.assertTrue(all(start > sp_day for ds, start, _ in fetched if ds == 'MODIS_NRT'))
        self.assertEqual(result['missing_dates'], [])
        self.assertFalse(any('unavailable' in failure for failure in result['failures']))

    def test_known_coverage_requires_successful_whole_area_source_ledger(self):
        day = date(2026, 10, 20)
        job = HistoricalJob.objects.create(
            status='completed', stage='completed', request_hash='coverage-known',
            start_date=day, end_date=day, result_payload={'datasets': [{
                'dataset': 'MODIS_SP', 'retrieved_dates': [day.isoformat()],
                'failed_dates': [], 'rejected_dates': [],
            }]},
        )
        self.assertEqual(HistoricalJobService._known_covered_dates('MODIS_SP', {day}, None, None), {day})
        job.status = 'partial'
        job.save(update_fields=['status'])
        self.assertEqual(HistoricalJobService._known_covered_dates('MODIS_SP', {day}, None, None), set())

    def test_kano_boundary_and_bbox_are_resolved_from_pinned_feature(self):
        region, geometry = resolve_region({
            'country': 'Nigeria',
            'type': 'state',
            'name': 'Kano',
        })
        self.assertEqual(region['boundary_code'], 'NG-KN')
        self.assertEqual(geometry['type'], 'Polygon')
        self.assertEqual(region['bbox'], {
            'south': 10.543358511785925,
            'west': 7.675170903614344,
            'north': 12.604603956107724,
            'east': 9.360311527055387,
        })
        self.assertTrue(point_in_geometry(8.5, 12.0, geometry))
        # This point is outside the polygon but still inside the Kano bbox.
        self.assertFalse(point_in_geometry(9.3, 11.0, geometry))
        self.assertTrue(point_in_geometry(7.750667224912547, 11.371434143870715, geometry))

    def test_other_administrative_regions_resolve_with_their_own_geometry(self):
        region, geometry = resolve_region({
            'country': 'Nigeria',
            'type': 'state',
            'name': 'Kaduna',
        })
        self.assertEqual(region['boundary_code'], 'NG-KD')
        self.assertEqual(geometry['type'], 'Polygon')
        self.assertFalse(point_in_geometry(8.5, 12.0, geometry))

    def test_point_in_polygon_handles_holes_and_multipolygons(self):
        polygon = {
            'type': 'Polygon',
            'coordinates': [
                [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]],
                [[4, 4], [6, 4], [6, 6], [4, 6], [4, 4]],
            ],
        }
        multi_polygon = {
            'type': 'MultiPolygon',
            'coordinates': [
                [[[20, 20], [21, 20], [21, 21], [20, 21], [20, 20]]],
                [[[30, 30], [31, 30], [31, 31], [30, 31], [30, 30]]],
            ],
        }
        self.assertTrue(point_in_geometry(2, 2, polygon))
        self.assertFalse(point_in_geometry(5, 5, polygon))
        self.assertTrue(point_in_geometry(20.5, 20.5, multi_polygon))
        self.assertFalse(point_in_geometry(25, 25, multi_polygon))

    def test_firms_dataset_aliases_and_five_day_date_chunks(self):
        self.assertEqual(_FIRMS_DATASETS['VIIRS_SNPP_NRT'][0], 'VIIRS_SNPP_NRT')
        self.assertEqual(_FIRMS_DATASETS['VIIRS_SNPP_SP'][0], 'VIIRS_SNPP_SP')
        self.assertEqual(_FIRMS_DATASETS['MODIS_NRT'][0], 'MODIS_NRT')
        chunks = list(date_chunks(date(2024, 1, 1), date(2024, 1, 12)))
        self.assertEqual(chunks, [
            (date(2024, 1, 1), 5),
            (date(2024, 1, 6), 5),
            (date(2024, 1, 11), 2),
        ])

    def test_firms_area_url_uses_official_bbox_order(self):
        url = build_firms_url(
            'test-key',
            'MODIS_SP',
            '2024-01-01',
            5,
            {'west': 7.6, 'south': 10.5, 'east': 9.4, 'north': 12.6},
        )
        self.assertIn('/MODIS_SP/7.6,10.5,9.4,12.6/5/2024-01-01', url)

    def test_firms_fetch_and_availability_helpers_use_mocked_responses(self):
        response = Mock()
        response.status_code = 200
        response.text = 'latitude,longitude,acq_date,acq_time,satellite\n1,2,2024-01-01,1200,T\n'
        response.raise_for_status.return_value = None
        session = Mock()
        session.get.return_value = response

        dataframe = fetch_dataset_chunk(
            'MODIS_SP',
            date(2024, 1, 1),
            1,
            {'west': 1, 'south': 2, 'east': 3, 'north': 4},
            'test-key',
            session=session,
            retries=0,
        )
        self.assertEqual(len(dataframe), 1)
        self.assertIn('/MODIS_SP/1,2,3,4/1/2024-01-01', session.get.call_args.args[0])
        self.assertEqual(session.get.call_args.kwargs['timeout'], 60)

        availability_response = Mock()
        availability_response.text = 'source,start_date,end_date\nMODIS_SP,2000-01-01,2026-10-24\n'
        availability_response.raise_for_status.return_value = None
        session.get.return_value = availability_response
        ranges = fetch_availability_ranges('MODIS_SP', 'test-key', session=session)
        self.assertEqual(ranges, [(date(2000, 1, 1), date(2026, 10, 24))])

    def test_firms_fetch_retries_timeouts_without_a_live_request(self):
        response = Mock()
        response.status_code = 200
        response.text = 'latitude,longitude,acq_date,acq_time,satellite\n'
        response.raise_for_status.return_value = None
        session = Mock()
        session.get.side_effect = [requests.exceptions.Timeout(), response]
        with patch('analysis.fetch_data.time.sleep'):
            dataframe = fetch_dataset_chunk(
                'MODIS_SP',
                date(2024, 1, 1),
                1,
                None,
                'test-key',
                session=session,
                retries=1,
            )
        self.assertTrue(dataframe.empty)
        self.assertEqual(session.get.call_count, 2)

    def test_firms_fetch_honors_retry_after_for_rate_limit(self):
        limited = Mock(status_code=429, headers={'Retry-After': '2'})
        available = Mock(status_code=200, text='latitude,longitude,acq_date,acq_time,satellite\n')
        available.raise_for_status.return_value = None
        session = Mock()
        session.get.side_effect = [limited, available]
        with patch('analysis.fetch_data.time.sleep') as sleep:
            dataframe = fetch_dataset_chunk(
                'MODIS_NRT', date(2026, 1, 1), 1, None, 'test-key', session=session, retries=1,
            )
        self.assertTrue(dataframe.empty)
        sleep.assert_called_once_with(2.0)


class HistoricalJobAPITests(APITransactionTestCase):
    def setUp(self):
        self.key_patch = patch.dict(os.environ, {'FIRMS_MAP_KEY': 'test-only-key'})
        self.availability_patch = patch(
            'fires.services.fetch_availability_ranges',
            return_value=[(date(2000, 1, 1), date(2030, 12, 31))],
        )
        self.fetch_patch = patch(
            'fires.services.fetch_dataset_chunk',
            side_effect=self._mock_firms_fetch,
        )
        self.key_patch.start()
        self.availability_patch.start()
        self.fetch_patch.start()
        self.addCleanup(self.key_patch.stop)
        self.addCleanup(self.availability_patch.stop)
        self.addCleanup(self.fetch_patch.stop)

    def _payload(self):
        return {
            'start_date': '2024-01-01',
            'end_date': '2024-01-02',
            'sensors': ['MODIS'],
            'bbox': {'south': -10, 'west': 10, 'north': 10, 'east': 20},
        }

    def _create_detection(self, acq_date, record_key):
        return FireDetection.objects.create(
            source_product='MODIS',
            sensor='MODIS',
            latitude=0,
            longitude=15,
            acq_date=acq_date,
            record_key=record_key,
        )

    def _mock_firms_fetch(self, dataset, start_date, day_range, bbox, map_key):
        rows = []
        for offset in range(day_range):
            day = start_date + timedelta(days=offset)
            if day not in {date(2024, 1, 1), date(2024, 1, 2)}:
                continue
            rows.append({
                'latitude': '0',
                'longitude': '15',
                'acq_date': day.isoformat(),
                'acq_time': '1200',
                'satellite': 'T',
                'brightness': '300',
                'frp': '1.5',
                'confidence': '80',
                'daynight': 'D',
                'version': '1',
            })
        return pd.DataFrame(rows, columns=[
            'latitude', 'longitude', 'acq_date', 'acq_time', 'satellite',
            'brightness', 'frp', 'confidence', 'daynight', 'version',
        ])

    def test_create_historical_job_returns_job_id(self):
        worker_started = threading.Event()
        release_worker = threading.Event()

        real_fetch_chunk = self._mock_firms_fetch

        def pause_before_fetch(dataset, start_date, day_range, bbox, map_key):
            worker_started.set()
            if not release_worker.wait(timeout=5):
                raise AssertionError('Test did not release the worker in time.')
            return real_fetch_chunk(dataset, start_date, day_range, bbox, map_key)

        with patch('fires.services.fetch_dataset_chunk', side_effect=pause_before_fetch):
            try:
                response = self.client.post(reverse('historical_jobs'), self._payload(), format='json')
                self.assertEqual(response.status_code, 202)
                self.assertIn('job_id', response.data)
                uuid.UUID(response.data['job_id'])
                self.assertEqual(response.data['status'], 'queued')
                self.assertEqual(response.data['stage'], 'queued')
                self.assertEqual(response.data['progress'], 0)
                self.assertTrue(worker_started.wait(timeout=5), 'Background worker did not start.')
                self.assertEqual(HistoricalJob.objects.count(), 1)
            finally:
                release_worker.set()

        self.assertTrue(HistoricalJobService.wait_for_background_job(response.data['job_id']))
        job = HistoricalJob.objects.get(job_id=response.data['job_id'])
        self.assertEqual(job.status, 'completed')

    def test_duplicate_historical_job_requests_are_deduplicated(self):
        with patch.object(HistoricalJobService, 'start_background_job') as start_worker:
            first = self.client.post(reverse('historical_jobs'), self._payload(), format='json')
            second = self.client.post(reverse('historical_jobs'), self._payload(), format='json')

        self.assertEqual(first.status_code, 202)
        self.assertEqual(second.status_code, 202)
        self.assertEqual(first.data['job_id'], second.data['job_id'])
        self.assertEqual(HistoricalJob.objects.count(), 1)
        start_worker.assert_called_once()

    def test_named_region_resolves_and_is_included_in_job_status(self):
        payload = {
            'start_date': '2024-01-01',
            'end_date': '2024-01-02',
            'sensors': ['MODIS'],
            'region': {'country': 'Nigeria', 'type': 'state', 'name': ' kano '},
        }
        with patch.object(HistoricalJobService, 'start_background_job'):
            job = HistoricalJobService.create_or_get_job(payload)

        response = self.client.get(reverse('historical_job_detail', args=[job.job_id]))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['region']['name'], 'Kano')
        self.assertEqual(response.data['region']['boundary_code'], 'NG-KN')
        self.assertEqual(response.data['region']['boundary_year'], '2022')
        self.assertAlmostEqual(response.data['region']['bbox']['west'], 7.675170903614344)
        self.assertAlmostEqual(response.data['region']['bbox']['south'], 10.543358511785925)
        self.assertEqual(response.data['bbox'], response.data['region']['bbox'])

    def test_region_selector_returns_pinned_state_names(self):
        response = self.client.get(reverse('administrative_regions'), {'country': 'NGA', 'type': 'state'})
        self.assertEqual(response.status_code, 200)
        self.assertIn('Kano', response.data['results'])
        self.assertEqual(response.data['boundary_year'], '2022')

    def test_unknown_named_region_is_rejected(self):
        payload = {
            'start_date': '2024-01-01',
            'end_date': '2024-01-02',
            'sensors': ['MODIS'],
            'region': {'country': 'Nigeria', 'type': 'state', 'name': 'Not a State'},
        }
        response = self.client.post(reverse('historical_jobs'), payload, format='json')
        self.assertEqual(response.status_code, 400)
        self.assertIn('Unknown Nigeria state', response.data['detail'])

    def test_different_named_regions_are_not_deduplicated_together(self):
        payload = {
            'start_date': '2024-01-01',
            'end_date': '2024-01-02',
            'sensors': ['MODIS'],
            'region': {'country': 'Nigeria', 'type': 'state', 'name': 'Kano'},
        }
        other_payload = {
            **payload,
            'region': {'country': 'Nigeria', 'type': 'state', 'name': 'Kaduna'},
        }
        with patch.object(HistoricalJobService, 'start_background_job'):
            first = HistoricalJobService.create_or_get_job(payload)
            second = HistoricalJobService.create_or_get_job(other_payload)
        self.assertNotEqual(first.job_id, second.job_id)

    def test_bbox_only_requests_remain_supported(self):
        with patch.object(HistoricalJobService, 'start_background_job'):
            job = HistoricalJobService.create_or_get_job(self._payload())
        self.assertIsNone(job.request_params['region'])
        self.assertEqual(job.request_params['bbox']['west'], 10.0)

    def test_analytics_and_hotspots_are_job_scoped_and_bounded(self):
        job = HistoricalJob.objects.create(
            status='completed',
            stage='completed',
            request_hash='analytics',
            start_date='2024-01-01',
            end_date='2024-01-02',
            actual_covered_dates=['2024-01-01', '2024-01-02'],
            result_payload={'requested_dates': ['2024-01-01', '2024-01-02']},
        )
        first = self._create_detection('2024-01-01', 'analytics-one')
        first.frp = 2.0
        first.daynight = 'D'
        first.save(update_fields=['frp', 'daynight'])
        second = self._create_detection('2024-01-02', 'analytics-two')
        second.frp = 4.0
        second.daynight = 'N'
        second.save(update_fields=['frp', 'daynight'])
        HistoricalJobDetection.objects.bulk_create([
            HistoricalJobDetection(job=job, detection=first),
            HistoricalJobDetection(job=job, detection=second),
        ])

        analytics = self.client.get(reverse('historical_job_analytics', args=[job.job_id]))
        self.assertEqual(analytics.status_code, 200)
        self.assertEqual(analytics.data['regional_totals']['detections'], 2)
        self.assertEqual(len(analytics.data['daily_detections']), 2)
        self.assertEqual(analytics.data['daily_frp'][0]['frp_average'], 2.0)
        self.assertEqual(analytics.data['day_night'][0]['daytime'], 1)

        hotspots = self.client.get(reverse('historical_job_hotspots', args=[job.job_id]), {'limit': 1})
        self.assertEqual(hotspots.status_code, 200)
        self.assertEqual(hotspots.data['count'], 2)
        self.assertEqual(len(hotspots.data['results']), 1)
        self.assertIn('latitude', hotspots.data['results'][0])
        invalid = self.client.get(reverse('historical_job_hotspots', args=[job.job_id]), {'limit': 501})
        self.assertEqual(invalid.status_code, 400)

    def test_static_chart_image_is_served(self):
        response = self.client.get(reverse('chart_image', args=['daily_fire_activity.png']))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'image/png')

    def test_historical_job_detail_returns_existing_job_fields(self):
        job = HistoricalJob.objects.create(
            status='processing',
            stage='processing',
            request_hash='detail-job',
            start_date='2024-01-01',
            end_date='2024-01-03',
            records_processed=7,
            actual_covered_dates=['2024-01-01'],
            missing_dates=['2024-01-02', '2024-01-03'],
            progress=60,
            error_summary='',
            result_payload={'records_processed': 7},
        )

        response = self.client.get(reverse('historical_job_detail', args=[job.job_id]))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['job_id'], str(job.job_id))
        self.assertEqual(response.data['status'], 'processing')
        self.assertEqual(response.data['stage'], 'processing')
        self.assertEqual(response.data['progress'], 60)
        self.assertEqual(response.data['records_processed'], 7)
        self.assertEqual(response.data['actual_covered_dates'], ['2024-01-01'])
        self.assertEqual(response.data['missing_dates'], ['2024-01-02', '2024-01-03'])
        self.assertIn('started_at', response.data)
        self.assertIn('finished_at', response.data)
        self.assertIn('updated_at', response.data)
        self.assertIn('error_summary', response.data)
        self.assertEqual(response.data['result'], {'records_processed': 7})

    def test_historical_job_detail_returns_404_for_unknown_job(self):
        response = self.client.get(
            reverse('historical_job_detail', args=['00000000-0000-0000-0000-000000000001'])
        )

        self.assertEqual(response.status_code, 404)

    def test_historical_job_detail_returns_completed_job_results(self):
        result = {
            'job_id': '00000000-0000-0000-0000-000000000001',
            'status': 'completed',
            'records_processed': 3,
            'actual_covered_dates': ['2024-01-01', '2024-01-02'],
            'missing_dates': [],
        }
        job = HistoricalJob.objects.create(
            status='completed',
            stage='completed',
            request_hash='completed-job',
            start_date='2024-01-01',
            end_date='2024-01-02',
            records_processed=3,
            actual_covered_dates=['2024-01-01', '2024-01-02'],
            result_payload=result,
        )

        response = self.client.get(reverse('historical_job_detail', args=[job.job_id]))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['status'], 'completed')
        self.assertEqual(response.data['records_processed'], 3)
        self.assertEqual(response.data['result'], result)

    def test_real_worker_persists_results_and_polling_reflects_progress(self):
        worker_processing = threading.Event()
        release_worker = threading.Event()
        worker_connection_closes = []
        real_persist = HistoricalJobService._persist_detections
        real_close = DatabaseWrapper.close

        def pause_during_persistence(job, detections):
            worker_processing.set()
            if not release_worker.wait(timeout=5):
                raise AssertionError('Test did not release the worker in time.')
            return real_persist(job, detections)

        def close_and_record_worker_connection(wrapper):
            real_close(wrapper)
            if threading.current_thread().name.startswith('historical-job-'):
                worker_connection_closes.append(wrapper.alias)

        with (
            patch.object(HistoricalJobService, '_persist_detections', side_effect=pause_during_persistence),
            patch.object(DatabaseWrapper, 'close', close_and_record_worker_connection),
            patch.object(threading, 'excepthook') as thread_exception,
        ):
            response = self.client.post(reverse('historical_jobs'), self._payload(), format='json')
            self.assertEqual(response.status_code, 202)
            job_id = response.data['job_id']
            self.assertTrue(worker_processing.wait(timeout=5), 'Worker did not reach processing.')
            processing = self.client.get(reverse('historical_job_detail', args=[job_id]))
            self.assertEqual(processing.status_code, 200)
            self.assertEqual(processing.data['status'], 'processing')
            self.assertEqual(processing.data['stage'], 'processing')
            release_worker.set()
            self.assertTrue(
                HistoricalJobService.wait_for_background_job(job_id, timeout=5),
                'Background worker did not finish before the timeout.',
            )

        completed = self.client.get(reverse('historical_job_detail', args=[job_id]))
        self.assertEqual(completed.status_code, 200)
        self.assertEqual(completed.data['status'], 'completed')
        self.assertEqual(completed.data['stage'], 'completed')
        self.assertEqual(completed.data['records_processed'], 2)
        self.assertEqual(completed.data['actual_covered_dates'], ['2024-01-01', '2024-01-02'])
        self.assertEqual(completed.data['missing_dates'], [])
        self.assertEqual(completed.data['result']['records_processed'], 2)
        self.assertEqual(completed.data['result']['records_fetched'], 2)
        self.assertEqual(HistoricalJobDetection.objects.filter(job_id=job_id).count(), 2)
        self.assertEqual(FireDetection.objects.count(), 2)
        self.assertIsNotNone(completed.data['finished_at'])
        self.assertGreaterEqual(len(worker_connection_closes), 2)
        self.assertTrue(all(alias == 'default' for alias in worker_connection_closes))
        thread_exception.assert_not_called()

    def test_worker_failure_is_recorded_without_uncaught_thread_exception(self):
        with patch.object(HistoricalJobService, 'start_background_job'):
            job = HistoricalJobService.create_or_get_job(self._payload())

        with (
            patch('fires.services.fetch_dataset_chunk', side_effect=RuntimeError('controlled fetch failure')),
            patch.object(threading, 'excepthook') as thread_exception,
        ):
            HistoricalJobService.start_background_job(str(job.job_id))
            self.assertTrue(
                HistoricalJobService.wait_for_background_job(str(job.job_id), timeout=5),
                'Failed worker did not finish before the timeout.',
            )

        job.refresh_from_db()
        self.assertEqual(job.status, 'failed')
        self.assertIn('controlled fetch failure', job.error_summary)
        thread_exception.assert_not_called()

    def test_named_region_worker_excludes_bbox_only_false_positives(self):
        payload = {
            'start_date': '2024-01-01',
            'end_date': '2024-01-02',
            'sensors': ['MODIS'],
            'region': {'country': 'Nigeria', 'type': 'state', 'name': 'Kano'},
        }
        data = pd.DataFrame([
            {
                'latitude': 12.0,
                'longitude': 8.5,
                'acq_date': '2024-01-01',
                'acq_time': '1200',
                'satellite': 'T',
                'brightness': 300,
                'frp': 1.5,
                'confidence': '80',
                'daynight': 'D',
                'version': '1',
            },
            {
                'latitude': 11.0,
                'longitude': 9.3,
                'acq_date': '2024-01-02',
                'acq_time': '1200',
                'satellite': 'T',
                'brightness': 300,
                'frp': 1.5,
                'confidence': '80',
                'daynight': 'D',
                'version': '1',
            },
        ])
        with patch('fires.services.fetch_dataset_chunk', return_value=data):
            response = self.client.post(reverse('historical_jobs'), payload, format='json')
            job_id = response.data['job_id']
            self.assertTrue(HistoricalJobService.wait_for_background_job(job_id, timeout=5))

        job = HistoricalJob.objects.get(job_id=job_id)
        self.assertEqual(job.status, 'completed')
        self.assertEqual(job.records_processed, 1)
        detection = HistoricalJobDetection.objects.get(job=job).detection
        self.assertEqual((detection.longitude, detection.latitude), (8.5, 12.0))

    def test_partial_firms_chunk_failure_is_reported_honestly(self):
        payload = {
            **self._payload(),
            'end_date': '2024-01-07',
        }
        fetch_attempt = 0

        def controlled_fetch(*args):
            nonlocal fetch_attempt
            fetch_attempt += 1
            if fetch_attempt == 1:
                raise RuntimeError('temporary chunk outage')
            return self._mock_firms_fetch(*args)

        with patch('fires.services.fetch_dataset_chunk', side_effect=controlled_fetch):
            response = self.client.post(reverse('historical_jobs'), payload, format='json')
            job_id = response.data['job_id']
            self.assertTrue(HistoricalJobService.wait_for_background_job(job_id, timeout=5))

        job = HistoricalJob.objects.get(job_id=job_id)
        self.assertEqual(job.status, 'partial')
        self.assertEqual(job.actual_covered_dates, ['2024-01-06', '2024-01-07'])
        self.assertEqual(job.missing_dates, ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'])
        self.assertIn('temporary chunk outage', job.error_summary)

    def test_retry_endpoint_queues_only_unresolved_date_spans(self):
        job = HistoricalJob.objects.create(
            status='partial', stage='partial', start_date=date(2024, 1, 1), end_date=date(2024, 1, 7),
            sensors=['MODIS'], products=[], missing_dates=['2024-01-02', '2024-01-03', '2024-01-07'],
            result_payload={'missing_dates': ['2024-01-02', '2024-01-03', '2024-01-07']},
            request_params={'bbox': {'south': -10, 'west': 10, 'north': 10, 'east': 20}},
        )
        with patch.object(HistoricalJobService, 'start_background_job'):
            response = self.client.post(reverse('historical_job_retry', args=[job.job_id]), {}, format='json')
        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.data['count'], 2)
        spans = list(HistoricalJob.objects.exclude(job_id=job.job_id).order_by('start_date').values_list('start_date', 'end_date'))
        self.assertEqual(spans, [(date(2024, 1, 2), date(2024, 1, 3)), (date(2024, 1, 7), date(2024, 1, 7))])

    def test_queued_job_starts_after_worker_slots_are_released(self):
        payloads = [
            self._payload(),
            {**self._payload(), 'start_date': '2024-02-01', 'end_date': '2024-02-02'},
            {**self._payload(), 'start_date': '2024-03-01', 'end_date': '2024-03-02'},
        ]
        with patch.object(HistoricalJobService, 'start_background_job'):
            jobs = [HistoricalJobService.create_or_get_job(payload) for payload in payloads]

        first_worker_started = threading.Event()
        queued_started = threading.Event()
        release_workers = threading.Event()
        active_workers = 0
        max_active_workers = 0
        active_lock = threading.Lock()
        def mocked_job_result(job):
            nonlocal active_workers, max_active_workers
            with active_lock:
                active_workers += 1
                max_active_workers = max(max_active_workers, active_workers)
            try:
                if job.job_id == jobs[2].job_id:
                    queued_started.set()
                elif job.job_id == jobs[0].job_id:
                    first_worker_started.set()
                    if not release_workers.wait(timeout=5):
                        raise AssertionError('Test did not release the occupied worker.')
                return {
                    'requested_dates': ['2024-01-01', '2024-01-02'],
                    'covered_dates': ['2024-01-01', '2024-01-02'],
                    'missing_dates': [],
                    'records_fetched': 0,
                    'records_processed': 0,
                    'records_inserted': 0,
                    'records_rejected': 0,
                    'datasets': [],
                    'failures': [],
                }
            finally:
                with active_lock:
                    active_workers -= 1

        try:
            with patch.object(HistoricalJobService, '_fetch_and_persist', side_effect=mocked_job_result):
                HistoricalJobService.start_background_job(str(jobs[0].job_id))
                HistoricalJobService.start_background_job(str(jobs[1].job_id))
                self.assertTrue(first_worker_started.wait(timeout=5))

                queued = self.client.post(
                    reverse('historical_jobs'),
                    payloads[2],
                    format='json',
                )
                self.assertEqual(queued.status_code, 202)
                self.assertEqual(queued.data['status'], 'queued')
                self.assertFalse(queued_started.is_set())

                release_workers.set()
                self.assertTrue(queued_started.wait(timeout=5), 'Queued worker was not dispatched.')
                for job in jobs:
                    self.assertTrue(
                        HistoricalJobService.wait_for_background_job(str(job.job_id), timeout=5),
                        'A worker did not finish before the timeout.',
                    )
        finally:
            release_workers.set()

        jobs[2].refresh_from_db()
        self.assertEqual(jobs[2].status, 'completed')
        self.assertLessEqual(max_active_workers, HistoricalJobService.THREAD_LIMIT)

    def test_historical_requests_can_reach_2000s(self):
        payload = {**self._payload(), 'start_date': '2000-01-01', 'end_date': '2000-01-05'}
        with patch.object(HistoricalJobService, 'start_background_job'):
            response = self.client.post(reverse('historical_jobs'), payload, format='json')
        self.assertEqual(response.status_code, 202)
        job = HistoricalJob.objects.get(job_id=response.data['job_id'])
        self.assertEqual(job.start_date, date(2000, 1, 1))

    def test_historical_persistence_remains_idempotent_with_small_sql_parameter_limit(self):
        job = HistoricalJob.objects.create(
            status='processing', stage='processing', request_hash='batching-test',
            start_date=date(2024, 1, 1), end_date=date(2024, 1, 1),
        )
        detections = [
            FireDetection(
                source_product='MODIS_SP', sensor='MODIS', latitude=index / 1000,
                longitude=10, acq_date=date(2024, 1, 1), record_key=f'historical-batch-{index}',
            )
            for index in range(37)
        ]
        with patch('fires.batching.max_sql_parameters', return_value=60):
            self.assertEqual(HistoricalJobService._persist_detections(job, detections), 37)
            self.assertEqual(HistoricalJobService._persist_detections(job, detections), 0)
        self.assertEqual(FireDetection.objects.filter(source_product='MODIS_SP').count(), 37)
        self.assertEqual(HistoricalJobDetection.objects.filter(job=job).count(), 37)


class LiveProcessLockTests(TestCase):
    def test_sqlite_live_writers_are_serialized_between_threads(self):
        entered = threading.Event()
        finished = threading.Event()

        def contender():
            with _live_database_process_lock():
                entered.set()
            finished.set()

        with _live_database_process_lock():
            worker = threading.Thread(target=contender)
            worker.start()
            self.assertFalse(entered.wait(0.1))
        self.assertTrue(entered.wait(2))
        self.assertTrue(finished.wait(2))
        worker.join(timeout=2)
        self.assertFalse(worker.is_alive())


class LiveRetrievalTests(APITransactionTestCase):
    def setUp(self):
        self.today = timezone.now().date()
        self.key = patch('fires.live.load_map_key', return_value='server-only-test-key')
        self.availability = patch(
            'fires.live.fetch_availability_ranges',
            return_value=[(self.today - timedelta(days=3), self.today)],
        )
        self.key.start()
        self.availability.start()
        self.addCleanup(self.key.stop)
        self.addCleanup(self.availability.stop)

    def _frame(self, latitude='1', acq_date=None):
        return pd.DataFrame([{
            'latitude': latitude,
            'longitude': '2',
            'acq_date': (acq_date or self.today).isoformat(),
            'acq_time': '1234',
            'satellite': 'T',
            'instrument': 'MODIS',
            'brightness': '310.5',
            'frp': '8.2',
            'confidence': '85',
            'daynight': 'D',
            'version': '6.1NRT',
        }])

    def _large_frame(self, count, duplicate_first=False):
        rows = []
        for index in range(count):
            rows.append({
                'latitude': str(1 + index / 1000),
                'longitude': '2',
                'acq_date': self.today.isoformat(),
                'acq_time': f'{index % 2400:04d}',
                'satellite': 'T',
                'instrument': 'MODIS',
                'brightness': str(310 + index / 10),
                'frp': str(8 + index / 10),
                'confidence': '85',
                'daynight': 'D',
                'version': '6.1NRT',
            })
        if duplicate_first:
            rows.extend([rows[0].copy(), rows[0].copy()])
        return pd.DataFrame(rows)

    def test_large_live_import_batches_all_nrt_products_and_skips_duplicates(self):
        frame = self._large_frame(127, duplicate_first=True)
        with patch('fires.batching.max_sql_parameters', return_value=60), patch(
            'fires.live.fetch_dataset_chunk', return_value=frame,
        ):
            for product, (sensor,) in LIVE_PRODUCTS.items():
                with self.subTest(product=product):
                    run, _ = LiveRetrievalService.create_or_get_run(product, 5)
                    LiveRetrievalService.run(run.pk)
                    run.refresh_from_db()
                    self.assertEqual(run.status, 'succeeded')
                    self.assertEqual(run.rows_fetched, len(frame))
                    self.assertEqual(run.rows_imported, 127)
                    self.assertEqual(run.duplicates_skipped, 2)
                    saved = FireDetection.objects.filter(source_product=product)
                    self.assertEqual(saved.count(), 127)
                    self.assertEqual(saved.first().sensor, sensor)
                    self.assertEqual(saved.first().acq_date, self.today)
                    self.assertIsNotNone(saved.first().retrieved_at)
                    coverage = CoverageLedger.objects.get(live_run=run, acquisition_date=self.today)
                    self.assertEqual(coverage.retrieval_outcome, 'succeeded')
                    self.assertEqual(coverage.observation_outcome, 'detections_returned')
                    self.assertEqual(coverage.detection_count, 129)

                    retry, _ = LiveRetrievalService.create_or_get_run(product, 5)
                    LiveRetrievalService.run(retry.pk)
                    retry.refresh_from_db()
                    self.assertEqual(retry.status, 'succeeded')
                    self.assertEqual(retry.rows_imported, 0)
                    self.assertEqual(retry.duplicates_skipped, len(frame))
                    self.assertEqual(saved.count(), 127)

    def test_failed_later_write_batch_keeps_prior_batches_and_retry_finishes_idempotently(self):
        frame = self._large_frame(139)
        real_persist = _persist_live_detection_batch
        calls = 0

        def fail_second_batch(run, detections, retrieved_at):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError('synthetic later-batch failure')
            return real_persist(run, detections, retrieved_at)

        with patch('fires.batching.max_sql_parameters', return_value=60), patch(
            'fires.live.fetch_dataset_chunk', return_value=frame,
        ), patch('fires.live._persist_live_detection_batch', side_effect=fail_second_batch):
            first, _ = LiveRetrievalService.create_or_get_run('MODIS_NRT', 5)
            LiveRetrievalService.run(first.pk)
        first.refresh_from_db()
        persisted_after_failure = FireDetection.objects.filter(source_product='MODIS_NRT').count()
        self.assertGreater(persisted_after_failure, 0)
        self.assertLess(persisted_after_failure, len(frame))
        self.assertEqual(first.rows_fetched, len(frame))
        self.assertEqual(first.rows_imported, persisted_after_failure)
        self.assertEqual(first.status, 'failed')
        self.assertTrue(first.failed_dates)

        with patch('fires.batching.max_sql_parameters', return_value=60), patch(
            'fires.live.fetch_dataset_chunk', return_value=frame,
        ):
            retry, _ = LiveRetrievalService.create_or_get_run('MODIS_NRT', 5)
            LiveRetrievalService.run(retry.pk)
        retry.refresh_from_db()
        self.assertEqual(retry.status, 'succeeded')
        self.assertEqual(retry.rows_imported, len(frame) - persisted_after_failure)
        self.assertEqual(retry.duplicates_skipped, persisted_after_failure)
        self.assertEqual(FireDetection.objects.filter(source_product='MODIS_NRT').count(), len(frame))

    def test_active_product_window_is_coalesced(self):
        first, created = LiveRetrievalService.create_or_get_run('MODIS_NRT', 5)
        second, created_again = LiveRetrievalService.create_or_get_run('MODIS_NRT', 5)
        self.assertTrue(created)
        self.assertFalse(created_again)
        self.assertEqual(first.pk, second.pk)

    def test_live_run_imports_records_and_repeat_refresh_skips_duplicate(self):
        with patch('fires.live.fetch_dataset_chunk', return_value=self._frame()):
            run, created = LiveRetrievalService.create_or_get_run('MODIS_NRT', 5)
            self.assertTrue(created)
            LiveRetrievalService.run(run.pk)
            run.refresh_from_db()
            self.assertEqual(run.status, 'succeeded')
            self.assertEqual(run.rows_imported, 1)
            row = FireDetection.objects.get(source_product='MODIS_NRT')
            self.assertIsNotNone(row.retrieved_at)
            self.assertEqual(row.acq_date, self.today)
            response = self.client.get(reverse('live_data_observations'))
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.data['count'], 1)
            self.assertEqual(response.data['results'][0]['source_product'], 'MODIS_NRT')
            self.assertIsNotNone(response.data['results'][0]['retrieved_at'])

            second, _ = LiveRetrievalService.create_or_get_run('MODIS_NRT', 5)
            LiveRetrievalService.run(second.pk)
            second.refresh_from_db()
            self.assertEqual(second.rows_imported, 0)
            self.assertEqual(second.duplicates_skipped, 1)
            self.assertEqual(FireDetection.objects.filter(source_product='MODIS_NRT').count(), 1)

    def test_successful_live_attempt_records_detection_and_confirmed_empty_day(self):
        with patch('fires.live.fetch_dataset_chunk', return_value=self._frame()):
            run, _ = LiveRetrievalService.create_or_get_run('MODIS_NRT', 1)
            LiveRetrievalService.run(run.pk)
        record = CoverageLedger.objects.get(live_run=run, acquisition_date=self.today)
        self.assertEqual(record.retrieval_outcome, 'succeeded')
        self.assertEqual(record.observation_outcome, 'detections_returned')
        self.assertEqual(record.detection_count, 1)

        empty, _ = LiveRetrievalService.create_or_get_run('VIIRS_SNPP_NRT', 1)
        with patch('fires.live.fetch_dataset_chunk', return_value=pd.DataFrame()):
            LiveRetrievalService.run(empty.pk)
        zero = CoverageLedger.objects.get(live_run=empty, acquisition_date=self.today)
        self.assertEqual(zero.retrieval_outcome, 'succeeded')
        self.assertEqual(zero.observation_outcome, 'zero_detections')
        self.assertEqual(zero.detection_count, 0)

    def test_availability_unavailable_and_check_failure_are_distinct(self):
        unavailable, _ = LiveRetrievalService.create_or_get_run('MODIS_NRT', 1)
        with patch('fires.live.fetch_availability_ranges', return_value=[]):
            LiveRetrievalService.run(unavailable.pk)
        absent = CoverageLedger.objects.get(live_run=unavailable, acquisition_date=self.today)
        self.assertEqual(absent.availability_outcome, 'unavailable')
        self.assertEqual(absent.retrieval_outcome, 'unavailable')
        self.assertEqual(absent.observation_outcome, 'unknown')

        failed, _ = LiveRetrievalService.create_or_get_run('VIIRS_SNPP_NRT', 1)
        with patch('fires.live.fetch_availability_ranges', side_effect=RuntimeError('availability timeout')):
            LiveRetrievalService.run(failed.pk)
        error = CoverageLedger.objects.get(live_run=failed, acquisition_date=self.today)
        self.assertEqual(error.availability_outcome, 'check_failed')
        self.assertEqual(error.retrieval_outcome, 'failed')
        self.assertEqual(error.observation_outcome, 'unknown')

    def test_fetch_parser_failure_and_retry_preserve_each_attempt(self):
        first, _ = LiveRetrievalService.create_or_get_run('MODIS_NRT', 1)
        with patch('fires.live.fetch_dataset_chunk', side_effect=ValueError('CSV parse failed')):
            LiveRetrievalService.run(first.pk)
        failed = CoverageLedger.objects.get(live_run=first, acquisition_date=self.today)
        self.assertEqual(failed.retrieval_outcome, 'failed')
        self.assertEqual(failed.observation_outcome, 'unknown')

        retry, _ = LiveRetrievalService.create_or_get_run('MODIS_NRT', 1)
        with patch('fires.live.fetch_dataset_chunk', return_value=self._frame()):
            LiveRetrievalService.run(retry.pk)
        self.assertEqual(CoverageLedger.objects.filter(
            source_product='MODIS_NRT', acquisition_date=self.today,
        ).count(), 2)

    def test_rejected_rows_create_partial_unknown_coverage(self):
        with patch('fires.live.fetch_dataset_chunk', return_value=self._frame(latitude='95')):
            run, _ = LiveRetrievalService.create_or_get_run('VIIRS_NOAA20_NRT', 1)
            LiveRetrievalService.run(run.pk)
        record = CoverageLedger.objects.get(live_run=run, acquisition_date=self.today)
        self.assertEqual(record.retrieval_outcome, 'partial')
        self.assertEqual(record.observation_outcome, 'unknown')
        self.assertEqual(record.rejected_count, 1)

    def test_invalid_coordinate_is_rejected_not_imported(self):
        with patch('fires.live.fetch_dataset_chunk', return_value=self._frame(latitude='95')):
            run, _ = LiveRetrievalService.create_or_get_run('MODIS_NRT', 5)
            LiveRetrievalService.run(run.pk)
        run.refresh_from_db()
        self.assertEqual(run.status, 'partial')
        self.assertEqual(run.rows_rejected, 1)
        self.assertEqual(run.rows_imported, 0)
        # Only the date of the rejected row is tainted; the other retrieved
        # dates in the same chunk are confirmed zero-detection dates.
        self.assertEqual(run.rejected_dates, [self.today.isoformat()])
        self.assertEqual(
            run.zero_detection_dates,
            [day for day in run.available_dates if day != self.today.isoformat()],
        )

    def test_refresh_endpoint_returns_run_ids_and_never_the_key(self):
        with patch('fires.live.LiveRetrievalService._start') as start:
            response = self.client.post(
                reverse('live_data_refresh'),
                {'products': ['MODIS_NRT'], 'window_days': 1},
                format='json',
            )
        self.assertEqual(response.status_code, 202)
        self.assertNotIn('server-only-test-key', str(response.data))
        self.assertEqual(response.data['runs'][0]['product'], 'MODIS_NRT')
        start.assert_called_once()
        from fires.views import live_data_refresh
        self.assertEqual(live_data_refresh.cls.throttle_scope, 'live_refresh')

    def test_live_status_reports_never_retrieved_as_stale(self):
        response = self.client.get(reverse('live_data_status'))
        self.assertEqual(response.status_code, 200)
        modis = next(item for item in response.data['products'] if item['product'] == 'MODIS_NRT')
        self.assertTrue(modis['stale'])
        self.assertIsNone(modis['last_successful_retrieval_at'])


class FireDetectionImportCommandTests(TestCase):
    def test_import_skips_rows_already_present_in_database_even_without_record_key_column(self):
        existing_row = {
            'source_product': 'MODIS',
            'sensor': 'Terra',
            'latitude': '1.0',
            'longitude': '2.0',
            'acq_date': '2024-01-01',
            'acq_time': '12:00',
            'brightness': '5.5',
            'frp': '10.0',
            'confidence_raw': '80',
            'daynight': 'D',
            'satellite': 'Aqua',
            'instrument': 'MODIS',
            'version': '1',
        }
        new_row = {
            'source_product': 'MODIS',
            'sensor': 'Terra',
            'latitude': '3.0',
            'longitude': '4.0',
            'acq_date': '2024-01-02',
            'acq_time': '13:00',
            'brightness': '6.5',
            'frp': '11.0',
            'confidence_raw': '90',
            'daynight': 'N',
            'satellite': 'Aqua',
            'instrument': 'MODIS',
            'version': '1',
        }

        record_key = _build_record_key(existing_row)
        FireDetection.objects.create(
            source_product=existing_row['source_product'],
            sensor=existing_row['sensor'],
            latitude=float(existing_row['latitude']),
            longitude=float(existing_row['longitude']),
            acq_date='2024-01-01',
            acq_time=existing_row['acq_time'],
            brightness=float(existing_row['brightness']),
            frp=float(existing_row['frp']),
            confidence_raw=existing_row['confidence_raw'],
            confidence_category=existing_row['confidence_raw'],
            daynight=existing_row['daynight'],
            satellite=existing_row['satellite'],
            instrument=existing_row['instrument'],
            version=existing_row['version'],
            record_key=record_key,
        )

        csv_path = Path('test_fire_import.csv')
        with csv_path.open('w', newline='', encoding='utf-8') as csv_file:
            writer = csv.DictWriter(
                csv_file,
                fieldnames=[
                    'source_product',
                    'sensor',
                    'latitude',
                    'longitude',
                    'acq_date',
                    'acq_time',
                    'brightness',
                    'frp',
                    'confidence_raw',
                    'daynight',
                    'satellite',
                    'instrument',
                    'version',
                ],
            )
            writer.writeheader()
            writer.writerow(existing_row)
            writer.writerow(new_row)

        call_command('import_fire_data', source=str(csv_path), batch_size=10)

        self.assertEqual(FireDetection.objects.filter(record_key=record_key).count(), 1)
        self.assertEqual(FireDetection.objects.count(), 2)

        csv_path.unlink(missing_ok=True)


class GlobalGeographyTests(TestCase):
    def _feature(self, name, coordinates=None, shape_id='TEST-1', shape_iso=None):
        coordinates = coordinates or [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]
        return {'type': 'Feature', 'properties': {'shapeName': name, 'shapeID': shape_id, 'shapeISO': shape_iso}, 'geometry': {'type': 'Polygon', 'coordinates': coordinates}}

    @patch('fires.geography._metadata_for', return_value={'boundaryID': 'TEST-ADM1', 'boundaryLicense': 'CC BY 4.0', 'boundaryYearRepresented': '2024'})
    @patch('fires.geography._download_feature_collection')
    def test_global_country_iso_and_case_insensitive_region_matching(self, download, _metadata):
        download.return_value = [self._feature('California')]
        region, geometry = resolve_region({'country': 'usa', 'type': 'state', 'name': 'CALIFORNIA'})
        self.assertEqual(region['country'], 'United States')
        self.assertEqual(region['country_code'], 'USA')
        self.assertEqual(region['administrative_level'], 'ADM1')
        self.assertTrue(point_in_geometry(5, 5, geometry))

    @patch('fires.geography._metadata_for', return_value={'boundaryID': 'TEST-ADM1'})
    @patch('fires.geography._download_feature_collection')
    def test_multiple_supported_country_adm1_names(self, download, _metadata):
        for country, iso, name in [('Canada', 'CAN', 'Ontario'), ('Brazil', 'BRA', 'Amazonas'), ('India', 'IND', 'Kerala')]:
            download.return_value = [self._feature(name)]
            region, geometry = resolve_region({'country': country, 'type': 'province' if country == 'Canada' else 'state', 'name': name.lower()})
            self.assertEqual(region['country_code'], iso)
            self.assertEqual(region['name'], name)

    def test_missing_country_and_unsupported_level_are_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Unknown or unsupported country'):
            resolve_region({'country': 'Mars', 'type': 'state', 'name': 'X'})
        with self.assertRaisesRegex(ValueError, 'administrative level'):
            resolve_region({'country': 'Nigeria', 'type': 'village', 'name': 'Kano'})

    @patch('fires.geography._metadata_for', return_value={'boundaryID': 'TEST'})
    @patch('fires.geography._download_feature_collection')
    def test_unknown_and_ambiguous_region_names(self, download, _metadata):
        download.return_value = [self._feature('Springfield', shape_id='A'), self._feature('Springfield', shape_id='B')]
        with self.assertRaisesRegex(ValueError, 'Ambiguous'):
            resolve_region({'country': 'USA', 'type': 'county', 'name': 'Springfield'})
        download.return_value = [self._feature('Other')]
        with self.assertRaisesRegex(ValueError, 'Unknown'):
            resolve_region({'country': 'USA', 'type': 'county', 'name': 'Missing'})

    def test_multipolygon_holes_and_boundary_points(self):
        polygon = {'type': 'Polygon', 'coordinates': [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]], [[2, 2], [4, 2], [4, 4], [2, 4], [2, 2]]]}
        self.assertTrue(point_in_geometry(0, 5, polygon))
        self.assertFalse(point_in_geometry(3, 3, polygon))
        multi = {'type': 'MultiPolygon', 'coordinates': [[[[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]]]], 'coordinates': []}
        # A separate MultiPolygon assertion is used because nested geometries are independently validated.
        multi = {'type': 'MultiPolygon', 'coordinates': [[[[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]]], [[[20, 20], [22, 20], [22, 22], [20, 22], [20, 20]]]]}
        self.assertTrue(point_in_geometry(21, 21, multi))
        self.assertFalse(point_in_geometry(10, 10, multi))

    def test_bounding_box_generation(self):
        polygon = {'type': 'Polygon', 'coordinates': [[[3, 4], [8, 4], [8, 9], [3, 9], [3, 4]]]}
        from fires.geography import _bbox
        self.assertEqual(_bbox(polygon), {'south': 4.0, 'west': 3.0, 'north': 9.0, 'east': 8.0})

    def test_country_name_iso_codes_and_alias_resolution(self):
        self.assertEqual(resolve_country('Nigeria')['iso3'], 'NGA')
        self.assertEqual(resolve_country('NGA')['iso3'], 'NGA')
        self.assertEqual(resolve_country('United States')['iso3'], 'USA')
        self.assertIsNone(resolve_country('ZZZ'))

    @patch('fires.geography.requests.get')
    def test_download_network_failure_is_a_clear_runtime_error(self, get):
        get.side_effect = requests.ConnectionError('offline')
        with tempfile.TemporaryDirectory() as directory:
            with override_settings(GEOBOUNDARIES_CACHE_DIR=directory):
                from fires.geography import _download_feature_collection
                with patch('fires.geography._metadata_for', return_value={'gjDownloadURL': 'https://example.test/boundary.geojson'}):
                    with self.assertRaisesRegex(RuntimeError, 'Could not download boundary dataset'):
                        _download_feature_collection('USA', 'ADM1')

    def test_invalid_cached_dataset_is_not_used(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory)
            cache_file = cache / 'USA_ADM1.geojson'
            cache_file.write_text('{"type":"NotFeatureCollection","features":[]}', encoding='utf-8')
            with override_settings(GEOBOUNDARIES_CACHE_DIR=cache):
                with patch('fires.geography._metadata_for', return_value={'gjDownloadURL': 'https://example.test/boundary.geojson'}), patch('fires.geography.requests.get') as get:
                    response = Mock()
                    response.raise_for_status.return_value = None
                    response.json.return_value = {'type': 'FeatureCollection', 'features': [self._feature('California')]}
                    get.return_value = response
                    from fires.geography import _download_feature_collection
                    features = _download_feature_collection('USA', 'ADM1')
                    self.assertEqual(features[0]['properties']['shapeName'], 'California')

from datetime import date, datetime, timedelta, timezone as dt_timezone
from unittest import mock

from django.test import SimpleTestCase, TestCase

from fires import live_scheduler as scheduler
from fires.live import LIVE_PRODUCTS
from fires.models import LiveRetrievalRun

NOW = datetime(2026, 11, 9, 12, 0, tzinfo=dt_timezone.utc)
PRODUCT = 'MODIS_NRT'


def add_run(status, requested_hours_ago, window_end=date(2026, 11, 9), product=PRODUCT):
    return LiveRetrievalRun.objects.create(
        product=product, sensor='MODIS', window_start=window_end - timedelta(days=4), window_end=window_end,
        status=status, requested_at=NOW - timedelta(hours=requested_hours_ago),
    )


@mock.patch.dict('os.environ', {'FIRESCOPE_LIVE_WINDOW_DAYS': '5'})
class DueRefreshTests(TestCase):
    def test_every_product_is_due_when_nothing_has_run(self):
        self.assertEqual(scheduler.due_refreshes(NOW), {product: 5 for product in LIVE_PRODUCTS})

    def test_recent_attempt_is_not_due_even_if_it_failed(self):
        add_run('failed', 1)
        self.assertNotIn(PRODUCT, scheduler.due_refreshes(NOW))

    def test_active_run_is_not_due(self):
        add_run('running', 10)
        self.assertNotIn(PRODUCT, scheduler.due_refreshes(NOW))

    def test_window_covers_the_days_missed(self):
        add_run('succeeded', 72, window_end=date(2026, 11, 6))
        self.assertEqual(scheduler.due_refreshes(NOW)[PRODUCT], 4)

    def test_window_stays_within_limits(self):
        add_run('succeeded', 4)
        self.assertEqual(scheduler.due_refreshes(NOW)[PRODUCT], 2)
        add_run('succeeded', 400, window_end=date(2026, 10, 1), product='VIIRS_SNPP_NRT')
        self.assertEqual(scheduler.due_refreshes(NOW)['VIIRS_SNPP_NRT'], 5)

    def test_tick_expires_orphans_then_queues_them_again(self):
        orphan = add_run('queued', 48)
        with mock.patch.object(scheduler.LiveRetrievalService, '_start') as start, mock.patch.object(scheduler, 'checkpoint'), mock.patch.object(scheduler, 'has_disk_space', return_value=True), self.captureOnCommitCallbacks(execute=True):
            scheduler.tick()
        orphan.refresh_from_db()
        self.assertEqual(orphan.status, 'failed')
        self.assertTrue(LiveRetrievalRun.objects.filter(product=PRODUCT, status='queued').exclude(pk=orphan.pk).exists())
        self.assertTrue(start.called)


    def test_low_disk_space_stops_new_refreshes(self):
        with mock.patch.object(scheduler, 'checkpoint'), mock.patch.object(scheduler, 'has_disk_space', return_value=False), mock.patch.object(scheduler.LiveRetrievalService, 'enqueue') as enqueue:
            self.assertEqual(scheduler.tick(), {})
        enqueue.assert_not_called()

    def test_journal_is_only_checkpointed_when_no_refresh_is_writing(self):
        add_run('running', 0)
        with mock.patch.object(scheduler, 'checkpoint') as checkpoint, mock.patch.object(scheduler, 'has_disk_space', return_value=False), mock.patch.object(scheduler, '_expire_stale_runs'):
            scheduler.tick()
        checkpoint.assert_not_called()


class CurrentDayDefaultTests(TestCase):
    def test_only_today_is_pulled_by_default(self):
        add_run('succeeded', 400, window_end=date(2026, 10, 1))
        with mock.patch.dict('os.environ', {}, clear=False) as env:
            env.pop('FIRESCOPE_LIVE_WINDOW_DAYS', None)
            due = scheduler.due_refreshes(NOW)
        self.assertEqual(set(due.values()), {1})


class ShouldRunTests(SimpleTestCase):
    def test_runs_only_in_the_serving_process(self):
        self.assertTrue(scheduler.should_run(['manage.py', 'runserver'], {'RUN_MAIN': 'true'}))
        self.assertFalse(scheduler.should_run(['manage.py', 'runserver'], {}))
        self.assertTrue(scheduler.should_run(['manage.py', 'runserver', '--noreload'], {}))

    def test_tests_and_commands_never_run_it(self):
        self.assertFalse(scheduler.should_run(['manage.py', 'test', 'fires'], {'RUN_MAIN': 'true'}))
        self.assertFalse(scheduler.should_run(['manage.py', 'refresh_live_data'], {}))

    def test_other_servers_can_opt_in(self):
        self.assertTrue(scheduler.should_run(['gunicorn'], {'FIRESCOPE_LIVE_SCHEDULER': '1'}))

    def test_disabled_by_zero_interval(self):
        with mock.patch.dict('os.environ', {'FIRESCOPE_LIVE_REFRESH_HOURS': '0'}):
            self.assertFalse(scheduler.start())

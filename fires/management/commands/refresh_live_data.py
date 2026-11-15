from django.core.management.base import BaseCommand, CommandError

from fires.live import LIVE_PRODUCTS, LiveRetrievalService
from fires.models import LiveRetrievalRun


class Command(BaseCommand):
    help = 'Refresh the latest available NASA FIRMS NRT observations for selected products.'

    def add_arguments(self, parser):
        parser.add_argument('--products', nargs='*', choices=tuple(LIVE_PRODUCTS), default=list(LIVE_PRODUCTS))
        parser.add_argument('--window-days', type=int, default=5)

    def handle(self, *args, **options):
        try:
            runs = LiveRetrievalService.enqueue(
                options['products'], options['window_days'], background=False,
            )
        except (ValueError, RuntimeError) as exc:
            raise CommandError(str(exc)) from exc

        # This command is suitable for a host scheduler: the scheduler process
        # performs bounded work and exits with an honest aggregate status.
        for queued in runs:
            run = LiveRetrievalRun.objects.get(pk=queued.pk)
            if run.status == 'queued':
                LiveRetrievalService.run(run.pk, dispatch_queued=False)
            run.refresh_from_db()
            self.stdout.write(
                f'{run.product}: {run.status}; acquisition={run.latest_acquisition_date or "none"}; '
                f'imported={run.rows_imported}; duplicates={run.duplicates_skipped}; '
                f'rejected={run.rows_rejected}'
            )
        if any(LiveRetrievalRun.objects.get(pk=run.pk).status in {'failed', 'partial'} for run in runs):
            raise CommandError('One or more live product refreshes failed or were partial; inspect live-data status.')

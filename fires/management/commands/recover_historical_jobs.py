from django.core.management.base import BaseCommand

from fires.services import HistoricalJobService


class Command(BaseCommand):
    help = 'Mark historical jobs left active by a previous process as interrupted.'

    def handle(self, *args, **options):
        recovered = HistoricalJobService.recover_interrupted_jobs()
        self.stdout.write(f'Marked {recovered} historical job(s) as interrupted.')

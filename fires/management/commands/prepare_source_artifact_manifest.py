from datetime import date
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from fires.source_artifacts import ARTIFACT_SETS, PILOT_DATE, write_manifest_template


class Command(BaseCommand):
    help = 'Create an offline manifest template for the bounded NASA source-artifact pilot.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--output',
            default='analysis/data/source_artifacts/pilot_20261018_central_kalimantan/manifest.json',
            help='Manifest path relative to the backend root, or an absolute local path.',
        )

    def handle(self, *args, **options):
        output = Path(options['output'])
        if not output.is_absolute():
            output = Path(settings.BASE_DIR) / output
        try:
            path = write_manifest_template(output)
        except FileExistsError as exc:
            raise CommandError(str(exc)) from exc
        cohort_root = path.parent
        for folder in ('original', 'metadata', 'derived', 'validation'):
            (cohort_root / folder).mkdir(exist_ok=True)
        year, month, day = PILOT_DATE.split('-')
        day_of_year = date.fromisoformat(PILOT_DATE).timetuple().tm_yday
        for set_id, spec in ARTIFACT_SETS.items():
            for role, product in spec['products'].items():
                if role == 'metadata':
                    continue
                (cohort_root / 'original' / set_id / product / spec['collection'] / year / f'{day_of_year:03d}').mkdir(parents=True, exist_ok=True)
        self.stdout.write(self.style.SUCCESS(f'Created unacquired pilot manifest: {path}'))

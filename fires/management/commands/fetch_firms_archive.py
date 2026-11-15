from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import requests
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from fires import firms_archive as archive

DEFAULT_ROOT = 'analysis/data/raw/firms_country'


class Command(BaseCommand):
    help = 'Download and verify FIRMS yearly per-country archive files. Safe to re-run: verified files are skipped.'

    def add_arguments(self, parser):
        parser.add_argument('--country', action='append', required=True, help='FIRMS country file name, e.g. Nigeria. Repeatable.')
        parser.add_argument('--product', action='append', choices=sorted(archive.PRODUCT_FIRST_YEAR), help='Defaults to all products.')
        parser.add_argument('--start-year', type=int, help="Defaults to each product's first year.")
        parser.add_argument('--end-year', type=int, help='Defaults to the current UTC year.')
        parser.add_argument('--root', default=DEFAULT_ROOT, help='Download directory, relative to the backend.')
        parser.add_argument('--force', action='store_true', help='Download again even if already verified.')

    def handle(self, *args, **options):
        try:
            countries = [archive.validate_country(name) for name in options['country']]
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        root = Path(options['root'])
        if not root.is_absolute():
            root = Path(settings.BASE_DIR) / root
        products = options['product'] or list(archive.PRODUCT_FIRST_YEAR)
        end_year = options['end_year'] or datetime.now(timezone.utc).year

        manifest = archive.load_manifest(root)
        tally: dict[str, int] = {}
        session = requests.Session()
        session.headers['User-Agent'] = 'FireScope-archive-fetch'

        for country in countries:
            for product in products:
                first_year = max(options['start_year'] or 0, archive.PRODUCT_FIRST_YEAR[product])
                for year in range(first_year, end_year + 1):
                    entry = archive.fetch_year(root, manifest, product, year, country, session, force=options['force'])
                    archive.save_manifest(root, manifest)
                    label = 'skipped' if entry.get('skipped') else entry['status']
                    tally[label] = tally.get(label, 0) + 1
                    detail = f"rows={entry['rows']} days={entry['days_with_detections']}" if entry.get('rows') else entry.get('error', '')
                    self.stdout.write(f'{country} {product} {year}: {label} {detail}'.rstrip())

        self.stdout.write(self.style.SUCCESS(f'Finished: {tally}. Manifest: {root / archive.MANIFEST_NAME}'))
        if tally.get('failed') or tally.get('invalid'):
            raise CommandError('Some files failed or were invalid. Re-run to retry; verified files are skipped.')

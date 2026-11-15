from __future__ import annotations

from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from fires import archive_aggregation as aggregation
from fires import firms_archive as archive
from fires.management.commands.fetch_firms_archive import DEFAULT_ROOT


class Command(BaseCommand):
    help = 'Aggregate verified FIRMS archive files into grid-cell daily counts. Safe to re-run.'

    def add_arguments(self, parser):
        parser.add_argument('--country', action='append', help='Limit to these FIRMS country names. Repeatable.')
        parser.add_argument('--root', default=DEFAULT_ROOT, help='Archive directory, relative to the backend.')
        parser.add_argument('--force', action='store_true', help='Rebuild files that are already aggregated.')

    def handle(self, *args, **options):
        root = Path(options['root'])
        if not root.is_absolute():
            root = Path(settings.BASE_DIR) / root

        entries = [
            entry for _, entry in sorted(archive.load_manifest(root)['files'].items())
            if entry['status'] == 'verified' and (not options['country'] or entry['country'] in options['country'])
        ]
        if not entries:
            raise CommandError(f'No verified archive files found under {root}.')

        failures = 0
        for entry in entries:
            label = f"{entry['country']} {entry['product']} {entry['year']}"
            try:
                outcome, source = aggregation.aggregate_manifest_entry(root, entry, force=options['force'])
            except (aggregation.AggregationError, OSError) as exc:
                failures += 1
                self.stderr.write(f'{label}: FAILED {exc}')
                continue
            self.stdout.write(f'{label}: {outcome} vegetation={source.vegetation_rows} rejected={source.rejected_rows}')

        if failures:
            raise CommandError(f'{failures} of {len(entries)} files failed. Re-run after fixing them.')
        self.stdout.write(self.style.SUCCESS(f'Finished {len(entries)} files.'))

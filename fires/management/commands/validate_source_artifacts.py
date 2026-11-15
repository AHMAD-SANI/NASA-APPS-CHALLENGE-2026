import json
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from fires.source_artifacts import validate_manifest


class Command(BaseCommand):
    help = 'Validate a local NASA source-artifact manifest and file integrity without network or database writes.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--manifest',
            default='analysis/data/source_artifacts/pilot_20261018_central_kalimantan/manifest.json',
            help='Manifest path relative to the backend root, or an absolute local path.',
        )

    def handle(self, *args, **options):
        manifest_path = Path(options['manifest'])
        if not manifest_path.is_absolute():
            manifest_path = Path(settings.BASE_DIR) / manifest_path
        try:
            manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise CommandError(f'Cannot read source artifact manifest: {exc}') from exc
        result = validate_manifest(manifest, manifest_path)
        self.stdout.write(json.dumps(result, indent=2))
        if not result['valid']:
            raise CommandError('Artifact manifest is incomplete or invalid; see validation errors above.')
        self.stdout.write(self.style.WARNING(
            'Manifest and file integrity are consistent. Product-internal QA/metadata inspection is still required.'
        ))

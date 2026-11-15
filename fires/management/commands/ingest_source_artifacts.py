import json
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from fires.source_ingestion import SourceProductError, ingest_manifest


class Command(BaseCommand):
    help = 'Parse local NASA Level-2 artifacts into an offline CSV staging bundle; never writes to the database.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--manifest',
            default='analysis/data/source_artifacts/pilot_20261018_central_kalimantan/manifest.json',
            help='Validated source artifact manifest path.',
        )
        parser.add_argument(
            '--output-dir',
            default='analysis/data/processed/source_ingestion/pilot_20261018_central_kalimantan',
            help='New output directory for CSV, preview and provenance manifest.',
        )
        parser.add_argument('--preview-rows', type=int, default=100)

    def handle(self, *args, **options):
        manifest_path = Path(options['manifest'])
        output_dir = Path(options['output_dir'])
        if not manifest_path.is_absolute():
            manifest_path = Path(settings.BASE_DIR) / manifest_path
        if not output_dir.is_absolute():
            output_dir = Path(settings.BASE_DIR) / output_dir
        try:
            manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
            result = ingest_manifest(manifest, manifest_path, output_dir, options['preview_rows'])
        except (OSError, UnicodeError, json.JSONDecodeError, SourceProductError) as exc:
            raise CommandError(str(exc)) from exc
        self.stdout.write(json.dumps(result, indent=2))
        self.stdout.write(self.style.WARNING(
            'Output is an offline staging bundle only. No FireDetection rows were inserted; real NASA compatibility remains unverified until source granules are tested.'
        ))

from __future__ import annotations

import time

from django.core.management.base import BaseCommand

from fires import harmonized
from fires.geography import get_region_details

NIGERIA = ('NGA', 'country', 'the Federal Republic of Nigeria')


class Command(BaseCommand):
    help = 'Pre-compute the harmonized index inputs for Nigeria. Run again after aggregating new archive files.'

    def handle(self, *args, **options):
        started = time.monotonic()
        details = get_region_details(*NIGERIA)
        area = harmonized.resolve_area(details['bbox'], details.get('polygon'))
        harmonized.load_series(area['cells'])
        self.stdout.write(self.style.SUCCESS(
            f"Cached {len(area['cells'])} cells ({area['status']}) in {time.monotonic() - started:.1f}s."
        ))

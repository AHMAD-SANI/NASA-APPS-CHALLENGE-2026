import csv
import os
import subprocess

project_root = r'C:\Users\Lenovo\Desktop\FireScope_Backend_APIs'
source_csv = os.path.join(project_root, 'analysis', 'data', 'processed', 'fire_detections.csv')
sample_csv = os.path.join(project_root, 'analysis', 'data', 'processed', 'fire_detections_sample.csv')
log_path = os.path.join(project_root, 'small_import_verification.txt')

with open(source_csv, newline='', encoding='utf-8') as src:
    reader = csv.DictReader(src)
    rows = [row for _, row in zip(range(25), reader)]

with open(sample_csv, 'w', newline='', encoding='utf-8') as dst:
    writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
    writer.writeheader()
    writer.writerows(rows)

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'FireScope_Backend.settings')
import django

django.setup()
from fires.models import FireDetection, DataImportRun

before = FireDetection.objects.count()
first = subprocess.run(['python', 'manage.py', 'import_fire_data', '--source', 'analysis/data/processed/fire_detections_sample.csv'], cwd=project_root, capture_output=True, text=True)
second = subprocess.run(['python', 'manage.py', 'import_fire_data', '--source', 'analysis/data/processed/fire_detections_sample.csv'], cwd=project_root, capture_output=True, text=True)

after_first = FireDetection.objects.count()
after_second = FireDetection.objects.count()

with open(log_path, 'w', encoding='utf-8') as log:
    log.write(f'BEFORE={before}\n')
    log.write(f'FIRST_RC={first.returncode}\n')
    log.write((first.stdout or '') + (first.stderr or ''))
    log.write(f'\nAFTER_FIRST={after_first}\n')
    log.write(f'SECOND_RC={second.returncode}\n')
    log.write((second.stdout or '') + (second.stderr or ''))
    log.write(f'\nAFTER_SECOND={after_second}\n')
    last = DataImportRun.objects.order_by('-started_at').first()
    if last:
        log.write(f'LAST_STATUS={last.status}\n')
        log.write(f'LAST_ROWS_READ={last.rows_read}\n')
        log.write(f'LAST_INSERTED={last.rows_inserted}\n')
        log.write(f'LAST_SKIPPED={last.rows_skipped}\n')
        log.write(f'LAST_FAILED={last.rows_failed}\n')

print(f'WROTE {log_path}')

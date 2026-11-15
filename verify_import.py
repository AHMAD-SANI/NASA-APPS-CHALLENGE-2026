import os
import subprocess
import sys

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'FireScope_Backend.settings')

project_root = r'C:\Users\Lenovo\Desktop\FireScope_Backend_APIs'
log_path = os.path.join(project_root, 'import_verification.txt')

with open(log_path, 'w', encoding='utf-8') as log:
    log.write('START\n')
    result = subprocess.run(['python', 'manage.py', 'import_fire_data'], cwd=project_root, capture_output=True, text=True)
    log.write(f'RC={result.returncode}\n')
    log.write((result.stdout or '') + (result.stderr or ''))

    import django
    django.setup()
    from fires.models import DataImportRun, FireDetection

    log.write(f'\nCOUNT={FireDetection.objects.count()}\n')
    last = DataImportRun.objects.order_by('-started_at').first()
    if last is not None:
        log.write(f'LAST_STATUS={last.status}\n')
        log.write(f'LAST_ROWS_READ={last.rows_read}\n')
        log.write(f'LAST_INSERTED={last.rows_inserted}\n')
        log.write(f'LAST_SKIPPED={last.rows_skipped}\n')
        log.write(f'LAST_FAILED={last.rows_failed}\n')
    else:
        log.write('LAST_STATUS=none\n')

print(f'WROTE {log_path}')

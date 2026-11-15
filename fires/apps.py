from django.apps import AppConfig
from django.db.backends.signals import connection_created


def _tune_sqlite(sender, connection, **kwargs):
    """Let readers run during live imports and keep the hot dataset cached.

    WAL lets dashboard reads proceed while a sync is writing (the default
    rollback journal blocks every reader for the length of each write).
    """
    if connection.vendor != 'sqlite':
        return
    with connection.cursor() as cursor:
        if connection.settings_dict['NAME'] not in (':memory:', '') and 'mode=memory' not in str(connection.settings_dict['NAME']):
            cursor.execute('PRAGMA journal_mode=WAL')
        cursor.execute('PRAGMA synchronous=NORMAL')
        cursor.execute('PRAGMA cache_size=-262144')      # 256 MB page cache
        cursor.execute('PRAGMA mmap_size=2147483648')    # 2 GB memory map
        cursor.execute('PRAGMA temp_store=MEMORY')


class FiresConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'fires'

    def ready(self):
        connection_created.connect(_tune_sqlite, dispatch_uid='fires_tune_sqlite')
        from fires import live_scheduler
        live_scheduler.start()

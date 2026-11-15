from django.contrib import admin

from .models import DailyFireSummary, DataImportRun, FireDetection, HistoricalJob


@admin.register(FireDetection)
class FireDetectionAdmin(admin.ModelAdmin):
    list_display = ('sensor', 'source_product', 'acq_date', 'latitude', 'longitude', 'confidence_category')
    list_filter = ('sensor', 'source_product', 'acq_date', 'daynight')
    search_fields = ('record_key', 'sensor', 'source_product', 'confidence_category')


@admin.register(DataImportRun)
class DataImportRunAdmin(admin.ModelAdmin):
    list_display = ('started_at', 'status', 'source_file', 'rows_read', 'rows_inserted', 'rows_failed')
    list_filter = ('status', 'source_file')


@admin.register(DailyFireSummary)
class DailyFireSummaryAdmin(admin.ModelAdmin):
    list_display = ('date', 'sensor', 'detection_count', 'daytime_count', 'nighttime_count', 'frp_total')
    list_filter = ('date', 'sensor')


@admin.register(HistoricalJob)
class HistoricalJobAdmin(admin.ModelAdmin):
    list_display = ('job_id', 'status', 'stage', 'start_date', 'end_date', 'records_processed', 'started_at')
    list_filter = ('status', 'stage')
    search_fields = ('job_id', 'error_summary', 'request_hash')

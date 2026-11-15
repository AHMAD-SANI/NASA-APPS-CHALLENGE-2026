from __future__ import annotations

import uuid

from django.core.exceptions import ValidationError
from django.db import models
from django.utils import timezone


class FireDetection(models.Model):
    source_product = models.CharField(max_length=120, db_index=True)
    sensor = models.CharField(max_length=120, db_index=True)
    latitude = models.FloatField(db_index=True)
    longitude = models.FloatField(db_index=True)
    acq_date = models.DateField(db_index=True)
    acq_time = models.CharField(max_length=32, blank=True, null=True)
    brightness = models.FloatField(blank=True, null=True, db_index=True)
    frp = models.FloatField(blank=True, null=True, db_index=True)
    scan = models.FloatField(blank=True, null=True)
    track = models.FloatField(blank=True, null=True)
    confidence_raw = models.CharField(max_length=64, blank=True, null=True)
    confidence_category = models.CharField(max_length=32, blank=True, null=True)
    daynight = models.CharField(max_length=8, blank=True, null=True)
    satellite = models.CharField(max_length=64, blank=True, null=True)
    instrument = models.CharField(max_length=64, blank=True, null=True)
    version = models.CharField(max_length=32, blank=True, null=True)
    record_key = models.CharField(max_length=255, unique=True, db_index=True)
    imported_at = models.DateTimeField(default=timezone.now)
    # Populated only for rows first persisted by an NRT refresh. Historical
    # acquisition time remains in acq_date/acq_time and is never overwritten.
    retrieved_at = models.DateTimeField(blank=True, null=True, db_index=True)

    class Meta:
        indexes = [
            models.Index(fields=['source_product', 'acq_date']),
            models.Index(fields=['sensor', 'acq_date']),
            models.Index(fields=['latitude', 'longitude']),
            models.Index(fields=['daynight', 'acq_date']),
            models.Index(fields=['confidence_category', 'sensor']),
            # Covering index for dashboard summaries/aggregations: product/date
            # range scans read these columns without touching table pages.
            models.Index(
                fields=['source_product', 'acq_date', 'retrieved_at', 'sensor', 'daynight', 'confidence_category', 'frp', 'latitude', 'longitude'],
                name='fd_dashboard_cover_idx',
            ),
            # Lets newest-first listings stop after one page instead of sorting.
            models.Index(fields=['-acq_date', '-acq_time', '-id'], name='fd_newest_first_idx'),
        ]

    def clean(self):
        super().clean()
        if self.confidence_category is not None:
            value = self.confidence_category.strip().lower()
            valid_values = {'n', 'l', 'h'}
            try:
                float(value)
            except ValueError:
                if value not in valid_values:
                    raise ValidationError(
                        {'confidence_category': 'Only MODIS numeric confidence values or VIIRS values n/l/h are supported.'}
                    )
        if self.daynight is not None:
            self.daynight = self.daynight.strip().upper()
            if self.daynight not in {'D', 'N'}:
                raise ValidationError({'daynight': 'Use D or N for day/night values.'})

    def __str__(self) -> str:
        return f'{self.sensor} @ {self.acq_date} ({self.record_key})'


class DataImportRun(models.Model):
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]

    started_at = models.DateTimeField(default=timezone.now)
    finished_at = models.DateTimeField(blank=True, null=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending', db_index=True)
    source_file = models.CharField(max_length=255, blank=True, null=True)
    import_description = models.TextField(blank=True, null=True)
    rows_read = models.PositiveBigIntegerField(default=0)
    rows_inserted = models.PositiveBigIntegerField(default=0)
    rows_skipped = models.PositiveBigIntegerField(default=0)
    rows_failed = models.PositiveBigIntegerField(default=0)
    error_message = models.TextField(blank=True, null=True)

    class Meta:
        ordering = ['-started_at']

    def __str__(self) -> str:
        return f'{self.status} @ {self.started_at:%Y-%m-%d %H:%M:%S}'


class DailyFireSummary(models.Model):
    date = models.DateField(db_index=True)
    sensor = models.CharField(max_length=120, db_index=True)
    detection_count = models.PositiveIntegerField(default=0)
    daytime_count = models.PositiveIntegerField(default=0)
    nighttime_count = models.PositiveIntegerField(default=0)
    frp_total = models.FloatField(default=0.0)
    frp_average = models.FloatField(default=0.0)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['date', 'sensor'], name='unique_daily_fire_summary_per_sensor')
        ]
        ordering = ['-date', 'sensor']

    def __str__(self) -> str:
        return f'{self.sensor} on {self.date}: {self.detection_count} detections'


class HistoricalJob(models.Model):
    STATUS_CHOICES = [
        ('queued', 'Queued'),
        ('fetching', 'Fetching'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('partial', 'Partial'),
        ('failed', 'Failed'),
        ('interrupted', 'Interrupted'),
        ('expired', 'Expired'),
    ]

    job_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='queued', db_index=True)
    stage = models.CharField(max_length=20, default='queued', db_index=True)
    request_hash = models.CharField(max_length=128, db_index=True, blank=True)
    request_params = models.JSONField(default=dict, blank=True)
    start_date = models.DateField(blank=True, null=True)
    end_date = models.DateField(blank=True, null=True)
    south = models.FloatField(blank=True, null=True)
    west = models.FloatField(blank=True, null=True)
    north = models.FloatField(blank=True, null=True)
    east = models.FloatField(blank=True, null=True)
    sensors = models.JSONField(default=list, blank=True)
    products = models.JSONField(default=list, blank=True)
    actual_covered_dates = models.JSONField(default=list, blank=True)
    missing_dates = models.JSONField(default=list, blank=True)
    records_processed = models.PositiveBigIntegerField(default=0)
    progress = models.PositiveSmallIntegerField(default=0)
    error_summary = models.TextField(blank=True, null=True)
    result_payload = models.JSONField(default=dict, blank=True)
    temp_dir = models.CharField(max_length=255, blank=True, null=True)
    expires_at = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(default=timezone.now)
    started_at = models.DateTimeField(default=timezone.now)
    finished_at = models.DateTimeField(blank=True, null=True)
    updated_at = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ['-started_at']

    @property
    def is_active(self):
        return self.status in {'queued', 'fetching', 'processing'}

    def as_api_dict(self):
        return {
            'job_id': str(self.job_id),
            'status': self.status,
            'stage': self.stage,
            'start_date': self.start_date.isoformat() if self.start_date else None,
            'end_date': self.end_date.isoformat() if self.end_date else None,
            'bbox': {
                'south': self.south,
                'west': self.west,
                'north': self.north,
                'east': self.east,
            },
            'region': self.request_params.get('region'),
            'sensors': self.sensors,
            'products': self.products,
            'records_processed': self.records_processed,
            'progress': self.progress,
            'actual_covered_dates': self.actual_covered_dates,
            'missing_dates': self.missing_dates,
            'error_summary': self.error_summary,
            'result': self.result_payload,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'finished_at': self.finished_at.isoformat() if self.finished_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'request_params': self.request_params,
        }

    def __str__(self) -> str:
        return f'{self.status} [{self.job_id}]'


class HistoricalJobDetection(models.Model):
    job = models.ForeignKey(HistoricalJob, on_delete=models.CASCADE, related_name='job_detections')
    detection = models.ForeignKey(FireDetection, on_delete=models.PROTECT, related_name='historical_jobs')
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['job', 'detection'],
                name='unique_detection_per_historical_job',
            )
        ]


class LiveRetrievalRun(models.Model):
    STATUS_CHOICES = [
        ('queued', 'Queued'),
        ('running', 'Running'),
        ('succeeded', 'Succeeded'),
        ('partial', 'Partial'),
        ('failed', 'Failed'),
    ]

    product = models.CharField(max_length=64, db_index=True)
    sensor = models.CharField(max_length=64, db_index=True)
    window_start = models.DateField()
    window_end = models.DateField()
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default='queued', db_index=True)
    requested_dates = models.JSONField(default=list, blank=True)
    available_dates = models.JSONField(default=list, blank=True)
    retrieved_dates = models.JSONField(default=list, blank=True)
    detection_dates = models.JSONField(default=list, blank=True)
    zero_detection_dates = models.JSONField(default=list, blank=True)
    rejected_dates = models.JSONField(default=list, blank=True)
    failed_dates = models.JSONField(default=list, blank=True)
    rows_fetched = models.PositiveBigIntegerField(default=0)
    rows_imported = models.PositiveBigIntegerField(default=0)
    duplicates_skipped = models.PositiveBigIntegerField(default=0)
    rows_rejected = models.PositiveBigIntegerField(default=0)
    error_summary = models.TextField(blank=True)
    requested_at = models.DateTimeField(default=timezone.now, db_index=True)
    started_at = models.DateTimeField(blank=True, null=True)
    finished_at = models.DateTimeField(blank=True, null=True)
    last_success_at = models.DateTimeField(blank=True, null=True, db_index=True)
    latest_acquisition_date = models.DateField(blank=True, null=True)

    class Meta:
        ordering = ['-requested_at']
        constraints = [
            models.UniqueConstraint(
                fields=['product', 'window_start', 'window_end'],
                condition=models.Q(status__in=['queued', 'running']),
                name='unique_active_live_retrieval_window',
            ),
        ]


class CoverageLedger(models.Model):
    """One immutable-by-attempt coverage result for a product/date/scope."""

    SCOPE_CHOICES = [
        ('global', 'Global'),
        ('bbox', 'Bounding box'),
        ('region', 'Named region'),
        ('unknown', 'Unknown scope'),
    ]
    RETRIEVAL_CHOICES = [
        ('unknown', 'Unknown'),
        ('succeeded', 'Succeeded'),
        ('partial', 'Partial'),
        ('unavailable', 'Product unavailable'),
        ('failed', 'Failed'),
    ]
    OBSERVATION_CHOICES = [
        ('unknown', 'Unknown'),
        ('detections_returned', 'Detections returned'),
        ('zero_detections', 'Confirmed zero detections'),
    ]
    AVAILABILITY_CHOICES = [
        ('unknown', 'Unknown'),
        ('available', 'Available'),
        ('unavailable', 'Unavailable'),
        ('check_failed', 'Availability check failed'),
    ]

    source_product = models.CharField(max_length=120, db_index=True)
    acquisition_date = models.DateField(db_index=True)
    scope_type = models.CharField(max_length=16, choices=SCOPE_CHOICES, default='unknown', db_index=True)
    scope_identifier = models.CharField(max_length=512, blank=True, default='')
    availability_outcome = models.CharField(
        max_length=16, choices=AVAILABILITY_CHOICES, default='unknown', db_index=True,
    )
    retrieval_outcome = models.CharField(max_length=16, choices=RETRIEVAL_CHOICES, default='unknown', db_index=True)
    observation_outcome = models.CharField(max_length=24, choices=OBSERVATION_CHOICES, default='unknown', db_index=True)
    detection_count = models.PositiveBigIntegerField(null=True, blank=True)
    rejected_count = models.PositiveBigIntegerField(null=True, blank=True)
    retrieved_at = models.DateTimeField(null=True, blank=True, db_index=True)
    live_run = models.ForeignKey(
        LiveRetrievalRun, null=True, blank=True, on_delete=models.SET_NULL,
        related_name='coverage_entries',
    )
    historical_job = models.ForeignKey(
        HistoricalJob, null=True, blank=True, on_delete=models.SET_NULL,
        related_name='coverage_entries',
    )
    attempt_key = models.CharField(max_length=255, unique=True)
    evidence = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now, db_index=True)

    class Meta:
        ordering = ['-acquisition_date', 'source_product', '-created_at']
        indexes = [
            models.Index(fields=['source_product', 'acquisition_date'], name='coverage_product_date_idx'),
            models.Index(fields=['scope_type', 'acquisition_date'], name='coverage_scope_date_idx'),
            models.Index(fields=['retrieval_outcome', 'acquisition_date'], name='coverage_retrieval_date_idx'),
            models.Index(fields=['observation_outcome', 'acquisition_date'], name='coverage_observ_date_idx'),
        ]
        constraints = [
            models.CheckConstraint(
                condition=~(models.Q(live_run__isnull=False) & models.Q(historical_job__isnull=False)),
                name='coverage_one_origin_max',
            ),
        ]


class ArchiveFile(models.Model):
    """One FIRMS yearly country file that has been aggregated."""

    country = models.CharField(max_length=120)
    product = models.CharField(max_length=32)
    year = models.PositiveSmallIntegerField()
    sha256 = models.CharField(max_length=64)
    aggregation_version = models.PositiveSmallIntegerField()
    grid_degrees = models.FloatField()
    rows_read = models.PositiveBigIntegerField()
    vegetation_rows = models.PositiveBigIntegerField()
    non_vegetation_rows = models.JSONField(default=dict)
    rejected_rows = models.PositiveBigIntegerField()
    # {platform: {first_date, last_date, detections, days_with_detections, complete_year}}
    platform_stats = models.JSONField(default=dict)
    aggregated_at = models.DateTimeField(default=timezone.now)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['country', 'product', 'year'], name='unique_archive_file'),
        ]


class ArchiveCellDay(models.Model):
    """Vegetation-fire detections for one platform, grid cell and UTC day.

    A missing row is not a confirmed zero: consult the source file's
    ``platform_stats`` for whether the platform was operating on that date.
    """

    source_file = models.ForeignKey(ArchiveFile, on_delete=models.CASCADE, related_name='cell_days')
    platform = models.CharField(max_length=16)
    acq_date = models.DateField()
    # South-west corner of the cell, as a multiple of ArchiveFile.grid_degrees.
    cell_lat = models.SmallIntegerField()
    cell_lon = models.SmallIntegerField()
    detections = models.PositiveIntegerField()
    daytime = models.PositiveIntegerField()
    nighttime = models.PositiveIntegerField()
    low_confidence = models.PositiveIntegerField()
    frp_sum = models.FloatField()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['platform', 'acq_date', 'cell_lat', 'cell_lon', 'source_file'],
                name='unique_archive_cell_day',
            ),
        ]

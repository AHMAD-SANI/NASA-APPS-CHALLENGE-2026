import django.utils.timezone
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('fires', '0002_historicaljob'),
    ]

    operations = [
        migrations.CreateModel(
            name='HistoricalJobDetection',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(default=django.utils.timezone.now)),
                ('detection', models.ForeignKey(
                    on_delete=django.db.models.deletion.PROTECT,
                    related_name='historical_jobs',
                    to='fires.firedetection',
                )),
                ('job', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='job_detections',
                    to='fires.historicaljob',
                )),
            ],
        ),
        migrations.AddConstraint(
            model_name='historicaljobdetection',
            constraint=models.UniqueConstraint(
                fields=('job', 'detection'),
                name='unique_detection_per_historical_job',
            ),
        ),
    ]

from __future__ import absolute_import, unicode_literals
import os
from celery.schedules import crontab
from celery import Celery
from django.conf import settings

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'etl.settings')

app = Celery('etl')
app.conf.enable_utc = False

app.conf.update(timezone = 'Asia/Kolkata')

app.config_from_object(settings, namespace='CELERY')


# CELERY BEAT settings
app.conf.beat_schedule = {
    'Update-local-db-at-12':{
        'task': 'dummyapp.tasks.test_func',
        'schedule': crontab(hour='17', minute='22'),
          'args': (2,) 
    }
}
  

app.autodiscover_tasks()

@app.task(bind = True)
def debug_task(self):
    print(f'Request: {self.request!r}')
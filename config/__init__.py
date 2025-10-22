# This ensures the celery app is always loaded when Django starts
from .celery import app as celery_app
from django.urls import path
from .views import FileUploadView, AnalysisStatusView

urlpatterns = [
    # file upload endpoint
    path('upload/', FileUploadView.as_view(), name='file-upload'),
    # task status query endpoint
    path('status/<str:job_id>/', AnalysisStatusView.as_view(), name='analysis-status'),
]
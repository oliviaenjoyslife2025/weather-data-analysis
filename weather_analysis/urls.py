from django.urls import path
from .views import FileUploadView, AnalysisStatusView, ListJobStatusesView, DeleteJobView

urlpatterns = [
    # file upload endpoint
    path('upload/', FileUploadView.as_view(), name='file-upload'),
    # task status query endpoint
    path('status/<str:job_id>/', AnalysisStatusView.as_view(), name='analysis-status'),
    # job statuses list endpoint
    path('job-statuses/', ListJobStatusesView.as_view(), name='job-statuses'),
    # delete job endpoint
    path('delete/<str:job_id>/', DeleteJobView.as_view(), name='delete-job'),
]
from django.urls import path
from .views import FileUploadView

urlpatterns = [
    # POST request to 'api/upload/' will be handled by FileUploadView
    path('upload/', FileUploadView.as_view(), name='file-upload'),
]
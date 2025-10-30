from rest_framework import serializers
from django.core.exceptions import ValidationError
import os

class FileUploadSerializer(serializers.Serializer):
    file = serializers.FileField()

    def validate_file(self, value):
        max_size = 50 * 1024 * 1024  # 50MB in bytes
        if value.size > max_size:
            raise serializers.ValidationError(
                f"File size too large. Maximum allowed size is 50MB. "
                f"Your file is {value.size / (1024 * 1024):.2f}MB."
            )
        
        # Check file extension
        allowed_extensions = ['.csv', '.xlsx', '.xls']
        file_extension = os.path.splitext(value.name)[1].lower()
        if file_extension not in allowed_extensions:
            raise serializers.ValidationError(
                f"Unsupported file type. Allowed types: {', '.join(allowed_extensions)}. "
                f"Your file type: {file_extension}"
            )
        
        # Check if file is empty
        if value.size == 0:
            raise serializers.ValidationError("File cannot be empty.")
        
        return value

    class Meta:
        fields = ['file']


class JobStatusSerializer(serializers.Serializer):
    job_id = serializers.CharField(max_length=64, min_length=64)
    status = serializers.CharField(max_length=20)
    timestamp = serializers.IntegerField(min_value=0)

    def validate_job_id(self, value):
        # Validate job_id is a valid SHA256 hash (64 characters, hex)
        if len(value) != 64:
            raise serializers.ValidationError("Job ID must be exactly 64 characters.")
        
        try:
            int(value, 16)  # Check if it's valid hex
        except ValueError:
            raise serializers.ValidationError("Job ID must be a valid hexadecimal string.")
        
        return value

    def validate_status(self, value):
        allowed_statuses = ['PENDING', 'RUNNING', 'SUCCESS', 'FAILURE', 'FAILED']
        if value not in allowed_statuses:
            raise serializers.ValidationError(
                f"Invalid status. Allowed values: {', '.join(allowed_statuses)}"
            )
        return value

    class Meta:
        fields = ['job_id', 'status', 'timestamp']


class AnalysisResultSerializer(serializers.Serializer):
    status = serializers.CharField(max_length=20)
    report_summary = serializers.CharField(max_length=1000)
    regression_analysis = serializers.DictField()
    num_records = serializers.IntegerField(min_value=0)
    time_series_data = serializers.ListField()

    def validate_status(self, value):
        allowed_statuses = ['SUCCESS', 'FAILURE']
        if value not in allowed_statuses:
            raise serializers.ValidationError(
                f"Invalid status. Allowed values: {', '.join(allowed_statuses)}"
            )
        return value

    def validate_num_records(self, value):
        if value < 0:
            raise serializers.ValidationError("Number of records cannot be negative.")
        return value

    class Meta:
        fields = ['status', 'report_summary', 'regression_analysis', 'num_records', 'time_series_data']
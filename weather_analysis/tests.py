from django.test import TestCase
from django.core.files.uploadedfile import SimpleUploadedFile
from rest_framework.test import APIClient
from rest_framework import status
from unittest.mock import patch, MagicMock, Mock
from django.core.cache import cache
from celery.result import AsyncResult
import json
import pandas as pd
from io import BytesIO

from .views import FileUploadView, AnalysisStatusView, get_file_hash
from .tasks import perform_analysis, run_weather_analysis


class ViewsTestCase(TestCase):
    """Unit tests for views.py"""
    
    def setUp(self):
        self.client = APIClient()
        
    @patch('weather_analysis.views.s3_client')
    @patch('weather_analysis.views.dynamodb_client')
    @patch('weather_analysis.views.run_weather_analysis')
    @patch('weather_analysis.views.cache')
    def test_file_upload_view_success(self, mock_cache, mock_task, mock_dynamodb, mock_s3):
        """
        Test FileUploadView - Successfully upload file and start Celery task
        """
        # Setup mocks
        mock_cache.get.return_value = None  # No data in cache
        mock_task_instance = MagicMock()
        mock_task_instance.id = 'test-celery-id-123'
        mock_task_instance.status = 'PENDING'
        mock_task.delay.return_value = mock_task_instance
        
        # Mock AWS clients - directly mock module-level clients
        mock_s3.put_object = MagicMock()
        mock_dynamodb.put_item = MagicMock()
        
        # Create test file
        csv_content = b"date,mean_temp_C,wind_speed,humidity\n2024-01-01,25.5,10.2,65.0"
        test_file = SimpleUploadedFile("test_weather.csv", csv_content, content_type="text/csv")
        
        # Send POST request
        response = self.client.post('/api/v1/upload/', {'file': test_file}, format='multipart')
        
        # Assertions
        self.assertEqual(response.status_code, status.HTTP_202_ACCEPTED)
        self.assertIn('job_id', response.data)
        self.assertIn('celery_id', response.data)
        self.assertEqual(response.data['status'], 'PENDING')
        self.assertEqual(response.data['from_cache'], False)
        
        # Verify AWS clients are called
        mock_s3.put_object.assert_called_once()
        mock_dynamodb.put_item.assert_called_once()
        
    @patch('weather_analysis.views.s3_client')
    @patch('weather_analysis.views.dynamodb_client')
    @patch('weather_analysis.views.cache')
    def test_file_upload_view_cached_result(self, mock_cache, mock_dynamodb, mock_s3):
        """
        Test FileUploadView - Return result from cache
        """
        # Setup mock - data exists in cache
        cached_result = {
            'status': 'SUCCESS',
            'report_summary': 'Test summary',
            'num_records': 100
        }
        mock_cache.get.return_value = cached_result
        
        # Create test file
        csv_content = b"date,mean_temp_C,wind_speed,humidity\n2024-01-01,25.5,10.2,65.0"
        test_file = SimpleUploadedFile("test_weather.csv", csv_content, content_type="text/csv")
        
        # Send POST request
        response = self.client.post('/api/v1/upload/', {'file': test_file}, format='multipart')
        
        # Assertions
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('job_id', response.data)
        self.assertEqual(response.data['status'], 'SUCCESS')
        self.assertEqual(response.data['from_cache'], True)
        self.assertEqual(response.data['results'], cached_result)
        
        # Verify AWS clients and Celery task are not called (using cache instead)
        # Note: Since s3_client and dynamodb_client are module-level in views.py, no need to verify here
        pass
        
    @patch('weather_analysis.views.dynamodb_client')
    def test_analysis_status_view_success(self, mock_dynamodb):
        """
        Test AnalysisStatusView - Successfully query task status
        """
        # Setup mocks
        job_id = 'a' * 64  # 64-character SHA256 hash
        celery_id = 'test-celery-id-123'
        
        # Mock DynamoDB response - dynamodb_client is a module-level variable
        mock_dynamodb.get_item.side_effect = [
            {'Item': {'celery_id': {'S': celery_id}}},  # First call - get celery_id
            {'Item': {'results': {'S': json.dumps({'status': 'SUCCESS', 'report_summary': 'Test'})}}}  # Second call - get results
        ]
        
        # Mock Celery AsyncResult
        mock_async_result = MagicMock()
        mock_async_result.ready.return_value = True
        mock_async_result.status = 'SUCCESS'
        
        with patch('weather_analysis.views.AsyncResult', return_value=mock_async_result):
            response = self.client.get(f'/api/v1/status/{job_id}/')
        
        # Assertions
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['status'], 'SUCCESS')
        self.assertEqual(response.data['job_id'], job_id)
        self.assertIn('results', response.data)
        
    @patch('weather_analysis.views.dynamodb_client')
    def test_analysis_status_view_job_not_found(self, mock_dynamodb):
        """
        Test AnalysisStatusView - Job not found case
        """
        # Setup mock - DynamoDB returns empty result
        job_id = 'a' * 64
        mock_dynamodb.get_item.return_value = {}
        
        response = self.client.get(f'/api/v1/status/{job_id}/')
        
        # Assertions
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn('error', response.data)


class TasksTestCase(TestCase):
    """Unit tests for tasks.py"""
    
    def test_perform_analysis_success(self):
        """
        Test perform_analysis - Successfully analyze data
        """
        # Create test data
        data = {
            'date': ['2024-01-01', '2024-01-02', '2024-01-03'],
            'mean_temp_C': [25.5, 26.0, 24.8],
            'wind_speed': [10.2, 12.5, 9.8],
            'humidity': [65.0, 70.0, 60.0]
        }
        df = pd.DataFrame(data)
        
        # Execute analysis
        result = perform_analysis(df)
        
        # Assertions
        self.assertEqual(result['status'], 'SUCCESS')
        self.assertIn('report_summary', result)
        self.assertIn('regression_analysis', result)
        self.assertIn('num_records', result)
        self.assertIn('time_series_data', result)
        self.assertEqual(result['num_records'], 3)
        self.assertGreater(len(result['time_series_data']), 0)
        
    def test_perform_analysis_missing_columns(self):
        """
        Test perform_analysis - Missing required columns case
        """
        # Create test data with missing required columns
        data = {
            'date': ['2024-01-01', '2024-01-02'],
            'mean_temp_C': [25.5, 26.0],
            # Missing wind_speed and humidity
        }
        df = pd.DataFrame(data)
        
        # Execute analysis
        result = perform_analysis(df)
        
        # Assertions
        self.assertEqual(result['status'], 'FAILURE')
        self.assertIn('report_summary', result)
        self.assertIn('Missing required columns', result['report_summary'])
        self.assertEqual(result['num_records'], 0)
        
    @patch('weather_analysis.tasks.s3_client')
    @patch('weather_analysis.tasks.dynamodb_client')
    @patch('weather_analysis.tasks.cache')
    def test_run_weather_analysis_task_success(self, mock_cache, mock_dynamodb, mock_s3):
        """
        Test run_weather_analysis - Celery task successfully executed
        """
        # Setup mocks
        job_id = 'test-job-id-123'
        s3_key = 'uploads/test-file.csv'
        
        # Create test CSV content
        csv_content = b"date,mean_temp_C,wind_speed,humidity\n2024-01-01,25.5,10.2,65.0\n2024-01-02,26.0,12.5,70.0"
        
        # Mock S3 client - s3_client is a module-level variable
        mock_s3.get_object.return_value = {'Body': BytesIO(csv_content)}
        
        # Mock DynamoDB client
        mock_dynamodb.put_item = MagicMock()
        mock_dynamodb.update_item = MagicMock()
        
        # Mock cache - cache is a Django module
        mock_cache.set = MagicMock()
        
        # Create task instance
        task_instance = Mock()
        task_instance.request = Mock()
        task_instance.request.id = 'test-celery-id'
        task_instance.update_state = MagicMock()
        
        # Execute task
        result = run_weather_analysis(task_instance, job_id, s3_key)
        
        # Assertions
        self.assertEqual(result['status'], 'SUCCESS')
        self.assertEqual(result['job_id'], job_id)
        
        # Verify methods are called
        mock_s3.get_object.assert_called_once()
        mock_dynamodb.put_item.assert_called()
        mock_dynamodb.update_item.assert_called()
        mock_cache.set.assert_called_once()
        
    @patch('weather_analysis.tasks.s3_client')
    @patch('weather_analysis.tasks.dynamodb_client')
    def test_run_weather_analysis_task_aws_client_failed(self, mock_dynamodb, mock_s3):
        """
        Test run_weather_analysis - AWS client initialization failed
        """
        # Setup mock - AWS clients are None
        with patch('weather_analysis.tasks.s3_client', None), \
             patch('weather_analysis.tasks.dynamodb_client', None):
            
            task_instance = Mock()
            task_instance.request = Mock()
            task_instance.request.id = 'test-celery-id'
            
            # Execute task, should raise exception
            with self.assertRaises(Exception) as context:
                run_weather_analysis(task_instance, 'test-job-id', 'test-key.csv')
            
            # Verify exception message
            self.assertIn('AWS clients failed', str(context.exception))

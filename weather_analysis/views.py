import os
import hashlib
import json
import boto3
import time 
from datetime import datetime, timedelta 

from django.conf import settings
from django.core.cache import cache
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from celery.result import AsyncResult
from .tasks import run_weather_analysis
from .serializers import FileUploadSerializer, JobStatusSerializer, AnalysisResultSerializer
import traceback
import sys

# AWS client initialization
def init_aws_clients():
    try:
        s3 = boto3.client(
            "s3", 
            region_name=settings.AWS_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )
        dynamodb = boto3.client(
            "dynamodb", 
            region_name=settings.AWS_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )
        return s3, dynamodb
    except Exception as e:
        print(f"[AWS CLIENT INIT ERROR] {type(e).__name__}: {e}")
        return None, None

s3_client, dynamodb_client = init_aws_clients()


# Helper functions
def get_file_hash(file_content: bytes) -> str:
    """Calculate SHA256 hash of file content as Job ID."""
    if not file_content:
        raise ValueError("File content is empty or invalid for hashing.")
    return hashlib.sha256(file_content).hexdigest()


class FileUploadView(APIView):
    """
    Handle file upload, record Job Metadata, and start Celery task.
    Returns job_id (file hash) and celery_id.
    """
    def post(self, request, *args, **kwargs):
        if not s3_client or not dynamodb_client:
            return Response({"error": "AWS clients are not initialized. Check server settings."}, status=500)

        try:
            # Validate file upload using serializer
            serializer = FileUploadSerializer(data=request.data)
            if not serializer.is_valid():
                return Response({
                    "error": "File validation failed.",
                    "details": serializer.errors
                }, status=400)

            file_obj = serializer.validated_data['file']
            file_content = file_obj.read()
            file_extension = os.path.splitext(file_obj.name)[1].lower()

            # Generate job ID, check cache, upload to S3 and start Celery task
            job_id = get_file_hash(file_content)
            s3_key = f"uploads/{job_id}{file_extension}"

            cache_key = f"analysis_result_{job_id}"
            cached_result = cache.get(cache_key)
            
            if cached_result:
                return Response(
                    {
                        "job_id": job_id,
                        "status": "SUCCESS",
                        "message": "📋 File already analyzed within 24 hours. Results retrieved from cache.",
                        "results": cached_result,
                        "from_cache": True,
                    },
                    status=status.HTTP_200_OK,
                )

            content_type_map = {
                '.csv': 'text/csv',
                '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                '.xls': 'application/vnd.ms-excel'
            }
            content_type = content_type_map.get(file_extension, file_obj.content_type or "application/octet-stream")

            s3_client.put_object(
                Bucket=settings.AWS_S3_BUCKET_NAME,
                Key=s3_key,
                Body=file_content,
                ContentType=content_type,
            )

            task = run_weather_analysis.delay(job_id, s3_key)
            dynamodb_client.put_item(
                TableName=settings.DYNAMODB_METADATA_TABLE_NAME, 
                Item={
                    'job_id': {'S': job_id},
                    'celery_id': {'S': task.id}, 
                    'status': {'S': task.status}, 
                    'timestamp': {'S': str(int(time.time()))},
                    's3_key': {'S': s3_key},
                }
            )

            return Response(
                {
                    "job_id": job_id,       # 文件哈希 (前端使用的主键)
                    "celery_id": task.id,   # Celery ID (后端查询实时状态)
                    "status": task.status,
                    "message": "✅ File uploaded to S3 and Celery job started successfully.",
                    "from_cache": False,
                },
                status=status.HTTP_202_ACCEPTED,
            )

        except Exception as e:
            print(f"[FileUpload ERROR] {type(e).__name__}: {e}")
            traceback.print_exc(file=sys.stdout)
            return Response(
                {"error": f"Failed to process file or start job: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


class AnalysisStatusView(APIView):
    """
    查询分析状态：阻塞模式。
    使用 job_id (文件哈希) 查找对应的 celery_id，阻塞等待任务完成，然后从 JobResults 获取结果。
    """
    def get(self, request, job_id, *args, **kwargs):
        if not dynamodb_client:
            return Response({"error": "AWS DynamoDB client not initialized."}, status=500)
        
        # Validate job_id format
        job_serializer = JobStatusSerializer(data={'job_id': job_id, 'status': 'PENDING', 'timestamp': 0})
        if not job_serializer.is_valid():
            return Response({
                "error": "Invalid job ID format.",
                "details": job_serializer.errors
            }, status=400)
        
        try:
            # Query job status and return results
            dynamodb_lookup = dynamodb_client.get_item(
                TableName=settings.DYNAMODB_METADATA_TABLE_NAME, 
                Key={'job_id': {'S': job_id}},
                ProjectionExpression='celery_id',
            )
            item = dynamodb_lookup.get('Item')
            
            if not item:
                return Response({"error": f"Job ID {job_id} not found."}, status=404)
            
            celery_id = item.get('celery_id', {}).get('S')
            if not celery_id:
                raise ValueError("Celery ID missing for this job in metadata.")
            
            celery_task_result = AsyncResult(celery_id)

            if not celery_task_result.ready():
                celery_task_result.wait(timeout=None, interval=1) 
            
            current_status = celery_task_result.status
            response_data = {
                "status": current_status,
                "job_id": job_id,
            }

            if current_status == 'SUCCESS':
                dynamodb_results_response = dynamodb_client.get_item(
                    TableName=settings.DYNAMODB_RESULTS_TABLE_NAME, 
                    Key={'job_id': {'S': job_id}} 
                )
                
                results_item = dynamodb_results_response.get('Item')
                
                if not results_item:
                    raise ValueError("Analysis results not found in JobResults table.")

                results_json_string = results_item.get('results', {}).get('S')
                if not results_json_string:
                     raise ValueError("Results data attribute missing in JobResults table item.")

                final_analysis_results = json.loads(results_json_string)

                response_data.update({
                    "message": "Analysis completed successfully (Fetched from JobResults).",
                    "results": final_analysis_results
                })
                return Response(response_data, status=status.HTTP_200_OK)

            elif current_status == 'FAILURE':
                error_details = str(celery_task_result.result)
                response_data.update({
                    "message": "Analysis failed.",
                    "error": error_details
                })
                return Response(response_data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
                
            else:
                return Response(response_data, status=status.HTTP_200_OK)

        except Exception as e:
            print(f"[STATUS FETCH ERROR] {type(e).__name__}: {e}")
            traceback.print_exc(file=sys.stdout)
            return Response(
                {"error": f"Failed to retrieve job status or result: {str(e)}", "status": "FAILURE"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class ListJobStatusesView(APIView):
    """
    列出过去 24 小时内所有任务的状态 (从 JobMetadata 表查询)。
    """
    def get(self, request, *args, **kwargs):
        if not dynamodb_client:
            return Response({"error": "AWS DynamoDB client not initialized."}, status=500)
            
        try:
            # Query job statuses from past 24 hours
            time_24_hours_ago = (datetime.now() - timedelta(hours=24)).timestamp()
            time_24_hours_ago_str = str(int(time_24_hours_ago))
            
            response = dynamodb_client.scan(
                TableName=settings.DYNAMODB_METADATA_TABLE_NAME, 
                FilterExpression='#t >= :cutoff',
                ProjectionExpression='job_id, #s, #t',
                ExpressionAttributeNames={
                    '#t': 'timestamp',
                    '#s': 'status'
                },
                ExpressionAttributeValues={
                    ':cutoff': {'S': time_24_hours_ago_str}
                }
            )

            job_statuses = []
            for item in response.get('Items', []):
                status_from_db = item.get('status', {}).get('S', 'UNKNOWN')
                job_id_from_db = item.get('job_id', {}).get('S', 'N/A')
                timestamp_from_db = item.get('timestamp', {}).get('S', '0')
                
                job_statuses.append({
                    'job_id': job_id_from_db,
                    'status': status_from_db,
                    'timestamp': int(timestamp_from_db)
                })

            job_statuses.sort(key=lambda x: x['timestamp'], reverse=True)
            
            return Response(job_statuses, status=status.HTTP_200_OK)

        except Exception as e:
            print(f"[LIST STATUSES ERROR] {type(e).__name__}: {e}")
            traceback.print_exc(file=sys.stdout)
            return Response(
                {"error": f"Failed to list job statuses: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class DeleteJobView(APIView):
    """
    Delete a specific job from both DynamoDB tables and Redis cache.
    """
    def delete(self, request, job_id, *args, **kwargs):
        if not dynamodb_client:
            return Response({"error": "AWS DynamoDB client not initialized."}, status=500)
        
        # Validate job_id format
        job_serializer = JobStatusSerializer(data={'job_id': job_id, 'status': 'PENDING', 'timestamp': 0})
        if not job_serializer.is_valid():
            return Response({
                "error": "Invalid job ID format.",
                "details": job_serializer.errors
            }, status=400)
        
        try:
            # Delete from JobMetadata table
            dynamodb_client.delete_item(
                TableName=settings.DYNAMODB_METADATA_TABLE_NAME,
                Key={'job_id': {'S': job_id}}
            )
            
            # Delete from JobResults table
            dynamodb_client.delete_item(
                TableName=settings.DYNAMODB_RESULTS_TABLE_NAME,
                Key={'job_id': {'S': job_id}}
            )
            
            # Delete from Redis cache
            cache_key = f"analysis_result_{job_id}"
            cache.delete(cache_key)
            
            return Response(
                {"message": f"Job {job_id} deleted successfully."},
                status=status.HTTP_200_OK
            )
            
        except Exception as e:
            print(f"[DELETE JOB ERROR] {type(e).__name__}: {e}")
            traceback.print_exc(file=sys.stdout)
            return Response(
                {"error": f"Failed to delete job: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
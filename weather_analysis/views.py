import os
import hashlib
import json
import boto3
from django.conf import settings
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from celery.result import AsyncResult
from django.core.cache import cache
from .tasks import run_weather_analysis
import traceback
import sys


def get_file_hash(file_content: bytes) -> str:
    if not file_content:
        raise ValueError("File content is empty or invalid for hashing.")
    return hashlib.sha256(file_content).hexdigest()

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

# Cache Timeout: 7 days
CACHE_TIMEOUT = 60 * 60 * 24 * 7


class FileUploadView(APIView):

    def post(self, request, *args, **kwargs):
        print("FILES:", request.FILES)
        print("DATA:", request.data)
        try:
            file_obj = request.FILES.get("file")
            if not file_obj:
                    raise Exception("No file uploaded")

            if isinstance(file_obj, list):
                file_obj = file_obj[0]

            file_extension = os.path.splitext(file_obj.name)[1].lower()
            if file_extension not in ['.csv', '.xlsx', '.xls']:
                return Response({
                    "error": f"Unsupported file type: {file_extension}. Please upload CSV or Excel files (.csv, .xlsx, .xls)"
                }, status=400)

            file_content = file_obj.read()
            if not file_content:
                return Response({"error": "Empty file"}, status=400)

            if not file_content or len(file_content) == 0:
                return Response({"error": "Uploaded file is empty."}, status=400)

            # --- generate job_id ---
            job_id = get_file_hash(file_content)
            print(f"[DEBUG] Generated job_id: {job_id}")

            # --- construct s3 key ---
            s3_key = f"uploads/{job_id}{file_extension}"

            # set correct content-type
            content_type_map = {
                '.csv': 'text/csv',
                '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                '.xls': 'application/vnd.ms-excel'
            }
            content_type = content_type_map.get(file_extension, file_obj.content_type or "application/octet-stream")

            if not s3_client:
                raise RuntimeError("AWS S3 client not initialized")

            s3_client.put_object(
                Bucket=settings.AWS_S3_BUCKET_NAME,
                Key=s3_key,
                Body=file_content,
                ContentType=content_type,
            )
        
            print(f"[DEBUG] File uploaded to S3 at key: {s3_key}")

            task = run_weather_analysis.delay(job_id, s3_key)
            print(f"[DEBUG] Celery task started with id: {task.id}")

            return Response(
                {
                    "job_id": task.id,
                    "job_id2": job_id,
                    "status": task.status,
                    "message": "✅ File uploaded to S3 and Celery job started successfully.",
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
    查询分析状态：已修改为阻塞模式（Blocking Mode）。
    该 GET 请求会阻塞（hold住）当前连接，直到 Celery 任务完成。
    """
    def get(self, request, job_id, *args, **kwargs):
        celery_task_result = AsyncResult(job_id)
        print("----------------------" + str(celery_task_result.ready()) + "----------------------" )
        if not celery_task_result.ready():
            print(f"[DEBUG] Blocking wait started for job_id: {job_id}")
            
            try:
                celery_task_result.wait(timeout=None, interval=1)
                print(f"[DEBUG] Blocking wait finished for job_id: {job_id}")
            except Exception as e:
                print(f"[CELERY WAIT ERROR] {type(e).__name__}: {e}")
                traceback.print_exc(file=sys.stdout)
                return Response(
                    {"error": f"Critical error while waiting for Celery task: {str(e)}", "status": "FAILURE"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
        
        # task is now guaranteed to be in a final state
        current_status = celery_task_result.status
        traceback.print_exc(file=sys.stdout)
        response_data = {
            "status": current_status,
            "job_id": job_id,
        }

        if current_status == 'SUCCESS':
            try:
                task_metadata = celery_task_result.result
                job_id_key = task_metadata.get('job_id') 
                
                if not job_id_key or not dynamodb_client:
                    raise RuntimeError("Missing job_id or DynamoDB client not initialized.")
                
                    # fetch results from DynamoDB
                dynamodb_response = dynamodb_client.get_item(
                    TableName=settings.AWS_DYNAMODB_TABLE_NAME, 
                    Key={'job_id': {'S': job_id_key}} 
                )
                
                item = dynamodb_response.get('Item')
                if not item:
                    raise ValueError("Analysis results not found in DynamoDB.")
                
                # fetch and deserialize stored JSON string
                results_json_string_attribute = item.get('results', {})  
                results_json_string = results_json_string_attribute.get('S')
                if not results_json_string:
                     raise ValueError("Results data attribute missing in DynamoDB item.")

                final_analysis_results = json.loads(results_json_string)

                response_data.update({
                    "message": "Analysis completed successfully (Fetched from DynamoDB).",
                    "results": final_analysis_results 
                })
                
                return Response(response_data, status=status.HTTP_200_OK)
                
            except Exception as e:
                print(f"[DYNAMODB FETCH ERROR] {type(e).__name__}: {e}")
                response_data.update({
                    "error": f"Task succeeded but failed to fetch/parse results from storage: {str(e)}",
                    "status": "FAILURE"
                })
                return Response(response_data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


        elif current_status == 'FAILURE':
            # task failed
            error_details = str(celery_task_result.result)
            response_data.update({
                "message": "Analysis failed.",
                "error": error_details
            })
            # return 500 INTERNAL SERVER ERROR
            return Response(response_data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
        else:
            # capture other final states (e.g. REVOKED, RETRY)
            error_details = str(celery_task_result.result) if celery_task_result.result else "Unknown error details."
            response_data.update({
                "message": f"Task ended with unexpected status: {current_status}",
                "error": error_details
            })
            return Response(response_data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
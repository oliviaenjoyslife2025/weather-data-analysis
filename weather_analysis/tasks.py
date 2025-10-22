import time
import json
import pandas as pd
import random
import boto3
from django.conf import settings
from config.celery import app
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from io import BytesIO 
import traceback
import sys

try:
    s3_client = boto3.client(
            "s3", 
            region_name=settings.AWS_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )
    dynamodb_client = boto3.client(
            "dynamodb", 
            region_name=settings.AWS_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )
    # s3_client = boto3.client('s3', region_name=settings.AWS_REGION)
    # dynamodb_client = boto3.client('dynamodb', region_name=settings.AWS_REGION)
except Exception as e:
    print(f"AWS CLIENT INITIALIZATION ERROR in TASK: {e}")
    traceback.print_exc(file=sys.stdout)
    s3_client = None
    dynamodb_client = None


def perform_analysis(df: pd.DataFrame) -> dict:
    features = ['mean_temp_C', 'wind_speed']
    df_clean = df.dropna(subset=features)

    summary = df_clean[features].describe().to_dict()

    try:
        scaler = StandardScaler()
        scaled_features = scaler.fit_transform(df_clean[features])
        
        kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
        df_clean['cluster'] = kmeans.fit_predict(scaled_features)
        
        cluster_summary = df_clean.groupby('cluster')[features].mean().to_dict()
    except Exception as e:
        cluster_summary = {"error": f"Clustering failed: {str(e)}"}
        traceback.print_exc(file=sys.stdout)

    return {
        "summary_statistics": summary,
        "cluster_analysis": cluster_summary,
        "num_records": len(df_clean)
    }

@app.task(bind=True)
def run_weather_analysis(self, job_id, s3_key):
    print("Begin to run run_weather_analysis task...")
    self.update_state(state='PROGRESS', meta={'progress': 10, 'message': 'Starting analysis...'})

    if not s3_client or not dynamodb_client:
        error_msg = "AWS clients failed to initialize in worker. Please check settings and credentials."
        self.update_state(state='FAILURE', meta={'error': error_msg})
        return {'status': 'FAILED', 'error': error_msg}

    try:
        self.update_state(state='PROGRESS', meta={'progress': 20, 'message': f'Downloading file from S3 key: {s3_key}'})
        print("Begin to download file from S3 key: {s3_key}")
        s3_object = s3_client.get_object(Bucket=settings.AWS_S3_BUCKET_NAME, Key=s3_key)
        file_content = s3_object['Body'].read()
        
        file_extension = s3_key.lower().split('.')[-1]
        
        if file_extension == 'csv':
            df = pd.read_csv(BytesIO(file_content))
        elif file_extension in ['xlsx', 'xls']:
            df = pd.read_excel(BytesIO(file_content))
        else:
            df = pd.read_csv(BytesIO(file_content))
        
        print("Begin to perform ML analysis...")
        self.update_state(state='PROGRESS', meta={'progress': 50, 'message': 'Performing ML analysis...'})
        analysis_results = perform_analysis(df)
        
        time.sleep(random.uniform(1, 3))
        
        print("Begin to save results to DynamoDB...")
        self.update_state(state='PROGRESS', meta={'progress': 90, 'message': 'Saving results to DynamoDB...'})
        results_json_string = json.dumps(analysis_results)
        
        dynamodb_client.put_item(
            TableName=settings.DYNAMODB_TABLE_NAME,
            Item={
                'job_id': {'S': job_id}, 
                'timestamp': {'S': str(int(time.time()))}, 
                's3_key': {'S': s3_key},
                'results': {'S': results_json_string} 
            }
        )
        
        return {
            'status': 'SUCCESS',
            'job_id': job_id,
            'results_key': s3_key
        }
        
    except Exception as e:
        error_msg = f"Task FAILED due to: {str(e)}"
        print(f"--- TASK {job_id} FAILED: {error_msg} ---")
        print(traceback.format_exc())
        self.update_state(state='FAILURE', meta={'error': error_msg, 'message': error_msg})
        return {'status': 'FAILED', 'error': error_msg}

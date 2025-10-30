import time
import json
import pandas as pd
import boto3
from django.conf import settings
from django.core.cache import cache
from config.celery import app
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression 
from io import BytesIO 
import traceback
import sys

# AWS client initialization
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
except Exception as e:
    print(f"AWS CLIENT INITIALIZATION ERROR in TASK: {e}")
    s3_client = None
    dynamodb_client = None


def perform_analysis(df: pd.DataFrame) -> dict:
    # Data validation, cleaning and type conversion
    clustering_features = ['mean_temp_C', 'wind_speed']
    regression_features = ['mean_temp_C', 'humidity']
    required_cols = list(set(clustering_features + regression_features + ['date']))
    
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        error_msg = f"Missing required columns: {', '.join(missing_cols)}."
        return {
            "status": "FAILURE", 
            "report_summary": f"FAILURE: {error_msg}",
            "regression_analysis": {"temp_humidity_r2": "N/A (Error)"},
            "num_records": 0,
            "time_series_data": [] 
        }
    df_clean = df.dropna(subset=clustering_features + regression_features)
    try:
        for col in clustering_features + regression_features:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce') 
    except Exception as e:
        error_msg = f"Data type conversion failed: {str(e)}"
        return {
            "status": "FAILURE",
            "report_summary": f"FAILURE: {error_msg}",
            "regression_analysis": {"temp_humidity_r2": "N/A (Error)"},
            "num_records": 0,
            "time_series_data": [] 
        }

    df_clean = df_clean.dropna(subset=clustering_features + regression_features)
    df_clean['date_dt'] = pd.to_datetime(df_clean['date'], errors='coerce') 
    df_clean = df_clean.dropna(subset=['date_dt']).drop(columns=['date']) 
    df_clean['date_str'] = df_clean['date_dt'].dt.strftime('%Y-%m-%d')
    
    num_records = len(df_clean)

    if num_records == 0:
        summary_text = "The dataset was empty after cleaning. No analysis performed."
        return {
            "status": "FAILURE",
            "report_summary": summary_text,
            "regression_analysis": {"temp_humidity_r2": "N/A (Empty Data)"},
            "num_records": 0,
            "time_series_data": []
        }

    # Linear regression R² calculation
    r_squared = 'N/A'
    try:
        X = df_clean[['humidity']]
        y = df_clean['mean_temp_C']
        
        if len(df_clean) > 1:
            model = LinearRegression()
            model.fit(X, y)
            r_squared_float = model.score(X, y) 
            r_squared = f"{r_squared_float:.4f}"
        
    except Exception as e:
        r_squared = f"Error: {str(e)}"
        
    # Extract time series data and generate report summary
    if num_records > 1000:
        df_chart = df_clean.iloc[::3, :]
    else:
        df_chart = df_clean
        
    time_series_data = df_chart[['date_str', 'mean_temp_C']].rename(
        columns={'date_str': 'date'}
    ).to_dict('records')

    start_date = df_clean['date_dt'].min().strftime('%Y-%m-%d')
    end_date = df_clean['date_dt'].max().strftime('%Y-%m-%d')
    
    avg_temp = df_clean['mean_temp_C'].mean() 
    
    summary_text = (
        f"This report covers {num_records} records from {start_date} to {end_date}. "
        f"The overall average temperature is {avg_temp:.2f}°C. "
    )
    return {
        "status": "SUCCESS", 
        "report_summary": summary_text,
        "regression_analysis": {
            "temp_humidity_r2": r_squared
        },
        "num_records": num_records,
        "time_series_data": time_series_data
    }


@app.task(bind=True)
def run_weather_analysis(self, job_id, s3_key):
    
    if not s3_client or not dynamodb_client:
        raise Exception("AWS clients failed to initialize in worker.")

    def update_ddb_status_failure():
        try:
            dynamodb_client.update_item(
                TableName=settings.DYNAMODB_METADATA_TABLE_NAME, 
                Key={'job_id': {'S': job_id}},
                UpdateExpression="SET #s = :status_val",
                ExpressionAttributeNames={'#s': 'status'},
                ExpressionAttributeValues={':status_val': {'S': 'FAILURE'}}
            )
        except Exception as e:
             print(f"Failed to update DDB status to FAILURE: {e}")
    
    try:
        # Process file and store results
        self.update_state(state='PROGRESS', meta={'progress': 20})
        s3_object = s3_client.get_object(Bucket=settings.AWS_S3_BUCKET_NAME, Key=s3_key)
        file_content = s3_object['Body'].read()
        
        file_extension = s3_key.lower().split('.')[-1]
        if file_extension == 'csv':
            df = pd.read_csv(BytesIO(file_content))
        elif file_extension in ['xlsx', 'xls']:
            df = pd.read_excel(BytesIO(file_content))
        else:
            df = pd.read_csv(BytesIO(file_content))
        
        self.update_state(state='PROGRESS', meta={'progress': 50})
        analysis_results = perform_analysis(df)
        
        if analysis_results.get('status') == 'FAILURE':
             raise Exception(f"Analysis failed during data processing: {analysis_results.get('report_summary')}")
        self.update_state(state='PROGRESS', meta={'progress': 90})
        results_json_string = json.dumps(analysis_results)
        
        dynamodb_client.put_item(
            TableName=settings.DYNAMODB_RESULTS_TABLE_NAME, 
            Item={
                'job_id': {'S': job_id}, 
                'results': {'S': results_json_string} 
            }
        )
        
        dynamodb_client.update_item(
            TableName=settings.DYNAMODB_METADATA_TABLE_NAME, 
            Key={'job_id': {'S': job_id}},
            UpdateExpression="SET #s = :status_val",
            ExpressionAttributeNames={'#s': 'status'},
            ExpressionAttributeValues={':status_val': {'S': 'SUCCESS'}}
        )
        
        cache_key = f"analysis_result_{job_id}"
        cache.set(cache_key, analysis_results, timeout=86400)
        
        return {
            'status': 'SUCCESS',
            'job_id': job_id,
        }
        
    except Exception as e:
        error_msg = f"Task FAILED: {str(e)}"
        print(f"--- TASK {self.request.id} FAILED: {error_msg} ---")
        traceback.print_exc()
        
        update_ddb_status_failure() # 更新 DDB 状态
             
        self.update_state(state='FAILURE', meta={'error': error_msg})
        raise # 必须重新抛出异常，让 Celery 记录失败状态
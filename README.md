# weather-data-analysis
Analyzing historical weather data using scikit-learn for linear regression analysis, time series extraction, and statistical reporting.

### Goals and Key Features
The primary objectives of this application are to provide fast, reliable, and asynchronous data analysis for various weather datasets.

1. File Upload & Storage: Users can upload data files (.csv, .xlsx, .xls) which are stored securely and durably in AWS S3.
2. Intelligent Caching: Implemented via Redis, the system checks the file content hash. If the file has been analyzed before, the results are returned instantly from the cache, bypassing the heavy analysis.
3. Asynchronous Analysis: Long-running analysis tasks are delegated to Celery Workers for background processing, ensuring a responsive user experience.
4. Machine Learning Analysis: Utilizes scikit-learn for data analysis tasks:
  - Linear Regression Analysis: Calculates the R² (coefficient of determination) between temperature and humidity to measure their correlation strength.
  - Time Series Data Extraction: Extracts date and temperature data for visualization purposes.
  - Statistical Summary Generation: Produces comprehensive reports including record count, date range, and average temperature statistics.
5. Persistent Results: All analysis findings are stored in AWS DynamoDB for fast retrieval.

### Technical Stack
Component	Technology	Role
- Storage	AWS S3	Durable storage for raw uploaded files.
- Caching	Redis	In-memory store for quick hash-based cache hits.
- Task Queue	Celery	Asynchronous task management for analysis jobs.
- Database	AWS DynamoDB	NoSQL store for structured analysis results.
- Analysis	Python, Pandas, scikit-learn	Data preprocessing and execution of ML models.
- Server	Django to handles API requests, S3, Redis, and Celery integration.

### Usage Workflow
- File Upload
1. The user uploads a file via the front-end.
2. Cache Check (Redis):
   - Cache Hit: If the file hash exists in Redis cache, the analysis results are immediately returned from Redis (no need to query DynamoDB).
   - Cache Miss: The file is uploaded to S3, and a Celery task is initiated.
3. Analysis and Status：
   - On cache miss, the server immediately returns a 202 Accepted response with the Job ID (file hash), Celery ID, and status PENDING.
   - A Celery task is dispatched: `run_weather_analysis.delay(job_id, s3_key)`.
   - The Celery Worker downloads the file from S3, performs the ML analysis, stores the results in DynamoDB JobResults table (key = job_id), updates the status in DynamoDB JobMetadata table, and caches the results in Redis (key = `analysis_result_{job_id}` with 24-hour expiration).
4. Retrieving Results:
   - The status endpoint uses blocking mode: it waits for the Celery task to complete before returning results.
   - When the status is SUCCESS, the endpoint fetches the final analysis results from DynamoDB JobResults table and returns them to the client.

### Installation and Setup
1. Clone this project
2. Required Python Libraries:
   ```
   pip install pandas openpyxl scikit-learn celery redis boto3
   ```
4. Create a .env file in the project root with your own configuration details:
```
#### Redis configuration
REDIS_HOST=localhost
REDIS_PORT=6379

#### Celery Broker (if using Redis for broker)
CELERY_BROKER_URL=redis://localhost:6379/0

#### AWS Configuration
AWS_ACCESS_KEY_ID=YOUR_KEY
AWS_SECRET_ACCESS_KEY=YOUR_SECRET
AWS_REGION=us-east-1
S3_BUCKET_NAME=your-weather-data-bucket
DYNAMODB_TABLE_NAME=WeatherAnalysisResults
```
4. Running Services (Local)
```
#### Start Redis 
brew services start redis  
```
5. Starting the Application
```
##### start the main server
python manage.py runserver
##### Start the Celery Worker:
celery -A config worker -l info 
```
6. url 
```
- `POST /api/v1/upload/` - File upload and job creation
- `GET /api/v1/status/{job_id}/` - Job status and results
- `GET /api/v1/job-statuses/` - List of recent jobs
- `DELETE /api/v1/delete/{job_id}/` - Delete specific job
```

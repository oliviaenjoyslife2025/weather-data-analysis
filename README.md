# weather-data-analysis
Analyzing historical weather data using scikit-learn for forecasting, trend detection, and pattern clustering.

### Goals and Key Features
The primary objectives of this application are to provide fast, reliable, and asynchronous data analysis for various weather datasets.

1. File Upload & Storage: Users can upload data files (.csv, .pdf, .xlsx) which are stored securely and durably in AWS S3.
2. Intelligent Caching: Implemented via Redis, the system checks the file content hash. If the file has been analyzed before, the results are returned instantly from the cache, bypassing the heavy analysis.
3. Asynchronous Analysis: Long-running analysis tasks are delegated to Celery Workers for background processing, ensuring a responsive user experience.
4. Machine Learning Analysis: Utilizes scikit-learn for three core analysis tasks:
  - Forecasting: Predicting tomorrow's temperature based on the past 7 days of data.
  - Climate Trend Detection: Identifying long-term warming trends (e.g., over 5 years) and detecting extreme temperature spikes.
  - Clustering: Grouping years with similar weather patterns using the KMeans algorithm.
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
   - Cache Hit: If the hash exists in Redis, the result is immediately retrieved from DynamoDB and returned.
   - Cache Miss: The file is uploaded to S3.
3. Analysis and Status：
   - The server immediately returns a 202 Accepted response with the Job ID (which is the file's hash) and status PENDING.
   - A Celery task is dispatched: analysis_task.delay(s3_path, file_hash).
   - The Celery Worker downloads the file, performs the ML analysis, stores the results in DynamoDB (key = hash), and updates Redis (key = hash, value = hash).
4. Retrieving Results (Front-End)
   - The front-end polls the status endpoint using the Job ID until the analysis is complete.
   - When the status is COMPLETED, the endpoint fetches the final analysis results from DynamoDB and sends them to the client for display.

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
#### Start Redis (Cache and Celery Broker)
docker-compose up -d redis
```
5. Starting the Application
```
##### start the main server
python manage.py runserver
##### Start the Celery Worker:
celery -A your_app_module worker -l info
```
6. url 
```
Upload fiel
http://127.0.0.1:8000/api/upload/
```

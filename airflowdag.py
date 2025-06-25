from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import boto3
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from dotenv import load_dotenv
import os
import pendulum
import time

# Load environment variables
load_dotenv(os.path.expanduser('~/airflow/.env'))

# AWS / S3 / Glue settings
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")
GLUE_JOB_NAME = 'job-name'
GLUE_SCRIPT_LOCATION = 's3://bucket/scripts/script.py'

# Lambda settings
NEWS_LAMBDA_FUNCTION = 'lambda-function'

# Snowflake settings
SNOWFLAKE_CONN_ID = 'snowflake_default'
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "snow_db")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "tables_schema")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "compute_wh")

# boto3 clients
lambda_client = boto3.client('lambda', region_name=AWS_REGION)
glue_client = boto3.client('glue', region_name=AWS_REGION)

def invoke_news_lambda():
    """Invoke the news-fetching Lambda synchronously."""
    try:
        print(f"🚀 Invoking Lambda function: {NEWS_LAMBDA_FUNCTION}")
        resp = lambda_client.invoke(
            FunctionName=NEWS_LAMBDA_FUNCTION,
            InvocationType='RequestResponse',
            LogType='Tail'
        )
        
        status_code = resp['StatusCode']
        payload = resp['Payload'].read().decode('utf-8')
        
        print(f"✅ Lambda invocation status: {status_code}")
        print(f"📄 Lambda response: {payload}")
        
        if status_code != 200:
            raise Exception(f"Lambda failed with status code: {status_code}")
            
        return payload
        
    except Exception as e:
        print(f"❌ Lambda invocation failed: {str(e)}")
        raise

def run_news_glue_job(**context):
    """Start the Glue job and XCom-push the JobRunId."""
    try:
        print(f"🚀 Starting Glue job: {GLUE_JOB_NAME}")
        resp = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                '--job-language': 'python',
                '--job-bookmark-option': 'job-bookmark-disable'
            },
            Timeout=30,
            NumberOfWorkers=2,
            WorkerType='G.1X'  # Updated for Glue version 5.0 compatibility
        )
        
        job_run_id = resp['JobRunId']
        print(f"✅ Started Glue job, run ID: {job_run_id}")
        
        # Push to XCom for potential use in downstream tasks
        context['ti'].xcom_push(key='glue_run_id', value=job_run_id)
        
        # Optional: Wait for job completion (uncomment if needed)
        # wait_for_glue_job_completion(job_run_id)
        
        return job_run_id
        
    except Exception as e:
        print(f"❌ Glue job failed to start: {str(e)}")
        raise

def wait_for_glue_job_completion(job_run_id, max_wait_time=1800):
    """Wait for Glue job to complete (optional function)."""
    print(f"⏳ Waiting for Glue job {job_run_id} to complete...")
    start_time = time.time()
    
    while time.time() - start_time < max_wait_time:
        response = glue_client.get_job_run(JobName=GLUE_JOB_NAME, RunId=job_run_id)
        job_run_state = response['JobRun']['JobRunState']
        
        print(f"📊 Job state: {job_run_state}")
        
        if job_run_state == 'SUCCEEDED':
            print("✅ Glue job completed successfully!")
            return True
        elif job_run_state in ['FAILED', 'STOPPED', 'ERROR', 'TIMEOUT']:
            error_msg = response['JobRun'].get('ErrorMessage', 'Unknown error')
            raise Exception(f"Glue job failed with state: {job_run_state}, Error: {error_msg}")
        
        time.sleep(30)  
    
    raise Exception(f"Glue job timed out after {max_wait_time} seconds")

def test_snowflake_connection():
    """Test Snowflake connection before loading data."""
    try:
        print("🔍 Testing Snowflake connection...")
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cs = conn.cursor()
        
        # Test basic connection
        cs.execute("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_ROLE()")
        result = cs.fetchone()
        print(f"✅ Connected to Snowflake!")
        print(f"   Version: {result[0]}")
        print(f"   User: {result[1]}")
        print(f"   Role: {result[2]}")
        
        # Test database and schema access
        cs.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cs.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
        print(f"✅ Successfully switched to {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
        
        # List tables to verify access
        cs.execute("SHOW TABLES")
        tables = cs.fetchall()
        print(f"📋 Found {len(tables)} tables in schema")
        
        return True
        
    except Exception as e:
        print(f"❌ Snowflake connection test failed: {str(e)}")
        raise
    finally:
        if 'cs' in locals():
            cs.close()
        if 'conn' in locals():
            conn.close()

def load_news_to_snowflake():
    """COPY INTO news_tables from S3 stage in Snowflake."""
    try:
        print("🚀 Starting Snowflake data load...")
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cs = conn.cursor()
        
        # Set context
        cs.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cs.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
        cs.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        
        print(f"📊 Using: {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA} with warehouse {SNOWFLAKE_WAREHOUSE}")
        
        # Check if stage exists
        cs.execute("SHOW STAGES LIKE 'small_s3_stage'")
        stages = cs.fetchall()
        if not stages:
            raise ValueError("Stage 'small_s3_stage' not found! Please create it first.")
        print("✅ Stage 'small_s3_stage' found")
        
        # Check if file format exists
        cs.execute("SHOW FILE FORMATS LIKE 'sm_csv_format'")
        formats = cs.fetchall()
        if not formats:
            raise ValueError("File format 'sm_csv_format' not found! Please create it first.")
        print("✅ File format 'sm_csv_format' found")
        
        # Check if table exists
        cs.execute("SHOW TABLES LIKE 'news_tables'")
        tables = cs.fetchall()
        if not tables:
            raise ValueError("Table 'news_tables' not found! Please create it first.")
        print("✅ Table 'news_tables' found")
        
        # Check for files in stage
        cs.execute("LIST @small_s3_stage")
        files = cs.fetchall()
        if not files:
            raise ValueError("No files found in stage 'small_s3_stage'")
        print(f"📂 Found {len(files)} files in stage 'small_s3_stage'")
        
        # Execute COPY command
        copy_sql = f"""
            COPY INTO news_tables
            FROM @small_s3_stage
            FILE_FORMAT = sm_csv_format
            PATTERN = '.*\\.csv'
            ON_ERROR = 'CONTINUE'
        """
        
        print("📥 Executing COPY command...")
        cs.execute(copy_sql)
        
        # Get copy results
        cs.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))")
        copy_results = cs.fetchall()
        print(f"📋 Raw copy results: {copy_results}")
        
        loaded_files = 0
        total_rows = 0
        errors = 0
        
        for result in copy_results:
            if len(result) >= 3:
                loaded_files += 1
                # Safely convert rows to integer
                try:
                    total_rows += int(result[1]) if result[1] is not None and str(result[1]).strip() != '' else 0
                except (ValueError, TypeError) as e:
                    print(f"⚠️  Warning: Could not convert rows value '{result[1]}' to integer: {str(e)}")
                    total_rows += 0
                
                # Safely convert errors to integer
                try:
                    errors += int(result[2]) if result[2] is not None and str(result[2]).strip() != '' else 0
                except (ValueError, TypeError) as e:
                    print(f"⚠️  Warning: Could not convert errors value '{result[2]}' to integer: {str(e)}")
                    errors += 0
        
        print(f"✅ Data load completed!")
        print(f"   Files processed: {loaded_files}")
        print(f"   Rows loaded: {total_rows}")
        print(f"   Errors: {errors}")
        
        if errors > 0:
            print("⚠️  Some errors occurred during load. Check Snowflake logs for details.")
        
        return {
            'files_processed': loaded_files,
            'rows_loaded': total_rows,
            'errors': errors
        }
        
    except Exception as e:
        print(f"❌ Snowflake data load failed: {str(e)}")
        print("🔍 Debugging info:")
        print(f"   Database: {SNOWFLAKE_DATABASE}")
        print(f"   Schema: {SNOWFLAKE_SCHEMA}")
        print(f"   Warehouse: {SNOWFLAKE_WAREHOUSE}")
        print(f"   Connection ID: {SNOWFLAKE_CONN_ID}")
        raise
    finally:
        if 'cs' in locals():
            cs.close()
        if 'conn' in locals():
            conn.close()

# Default arguments for all tasks
default_args = {
    'owner': 'sanisha',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': pendulum.datetime(2025, 6, 17, tz="Asia/Kolkata"),
}

# Create the DAG
with DAG(
    dag_id='full_news_pipeline',
    default_args=default_args,
    start_date=pendulum.datetime(2025, 6, 17, tz="Asia/Kolkata"),
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    description='ETL pipeline: Lambda → Glue → Snowflake',
    tags=['news', 'etl', 'glue', 'snowflake'],
    max_active_runs=1,
    doc_md="""
    # News Pipeline DAG
    
    This DAG orchestrates a complete ETL pipeline:
    
    1. **invoke_lambda**: Calls AWS Lambda to fetch news data
    2. **start_glue_job**: Starts AWS Glue job for data processing
    3. **test_snowflake_connection**: Tests Snowflake connectivity
    4. **load_into_snowflake**: Loads processed data into Snowflake
    
    ## Prerequisites
    - AWS Lambda function 'news-function' must exist
    - AWS Glue job 'airflow-job' must be configured
    - Snowflake stage 'small_s3_stage' must be created
    - Snowflake file format 'sm_csv_format' must be defined
    - Snowflake table 'news_tables' must exist
    """
) as dag:

    # Task 1: Invoke Lambda function
    invoke_lambda = PythonOperator(
        task_id='invoke_lambda',
        python_callable=invoke_news_lambda,
        doc_md="▶ Invoke the news-fetching Lambda function"
    )

    # Task 2: Start Glue job
    start_glue_job = PythonOperator(
        task_id='start_glue_job',
        python_callable=run_news_glue_job,
        provide_context=True,
        doc_md="▶ Trigger the AWS Glue job and XCom its run ID"
    )

    # Task 3: Test Snowflake connection
    test_connection = PythonOperator(
        task_id='test_snowflake_connection',
        python_callable=test_snowflake_connection,
        doc_md="▶ Test Snowflake connection and verify setup"
    )

    # Task 4: Load data into Snowflake
    load_into_snowflake = PythonOperator(
        task_id='load_into_snowflake',
        python_callable=load_news_to_snowflake,
        doc_md="▶ COPY INTO Snowflake from S3 stage"
    )

    # Define task dependencies
    invoke_lambda >> start_glue_job >> test_connection >> load_into_snowflake

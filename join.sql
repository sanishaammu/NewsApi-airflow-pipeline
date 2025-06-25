CREATE OR REPLACE DATABASE snow_db;
USE DATABASE snow_db;
CREATE OR REPLACE SCHEMA tables_schema;
USE SCHEMA tables_schema;
CREATE OR REPLACE TABLE news_tables(
  title STRING,
  source STRING,
  publishedAt TIMESTAMP_NTZ,
  url STRING
);

CREATE OR REPLACE TABLE news_report_table (
  title STRING,
  source STRING,
  publishedAt TIMESTAMP_NTZ,
  url STRING,
  source_description STRING,       -- Joined from another Snowflake table
  report_generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


  CREATE OR REPLACE STORAGE INTEGRATION sm_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn'
  STORAGE_ALLOWED_LOCATIONS = ('s3://bucket/output/');

  
  CREATE OR REPLACE STAGE small_s3_stage
  URL = 's3://bucket/output/'
  STORAGE_INTEGRATION = sm_integration;

  CREATE OR REPLACE FILE FORMAT sm_csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 0  -- Don't skip any rows
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ESCAPE_UNENCLOSED_FIELD = NONE
  NULL_IF = ('\\N', 'NULL', '')
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- COPY INTO news_table
-- FROM @my_s3_stage/kafka_data/part-00000-b2f39269-9227-4c59-a0c6-4f5e715a7354-c000.csv
-- FILE_FORMAT = my_csv_format
-- ON_ERROR = 'CONTINUE';

COPY INTO news_tables
FROM @small_s3_stage
FILE_FORMAT = sm_csv_format
PATTERN = '.*\.csv'
ON_ERROR = 'CONTINUE';

COPY INTO news_report_table
FROM @small_s3_stage
FILE_FORMAT = sm_csv_format
PATTERN = '.*\.csv'
ON_ERROR = 'CONTINUE';


DESC INTEGRATION sm_integration;



LIST @small_s3_stage;

SELECT * FROM news_tables;
SELECT * FROM news_report_table;

LIST @small_s3_stage;

import boto3
import pandas as pd
import snowflake.connector
from io import StringIO

def generate_report():
    print("🚀 Starting report generation...")

    # --- Step 1: Read S3 data ---
    print("🔍 Reading data from S3...")
    s3 = boto3.client(
        's3',
        aws_access_key_id='access-key',
        aws_secret_access_key='secret-key'
    )

    print("📂 Listing files in S3 bucket 'bucket-name'...")
    response = s3.list_objects_v2(Bucket='bucket-name')
    if 'Contents' not in response:
        print("❌ No files found in bucket. Exiting.")
        return

    for obj in response['Contents']:
        print("📄 Found file in S3:", obj['Key'])

    key = 'news-output/part.json'

    try:
        obj = s3.get_object(Bucket='bucket-name', Key=key)
        s3_data = pd.read_json(obj['Body'], lines=True)
        print("✅ Successfully loaded S3 data:")
        print(s3_data.head())
    except s3.exceptions.NoSuchKey:
        print(f"❌ ERROR: The key '{key}' was not found in the bucket.")
        return
    except Exception as e:
        print("❌ ERROR: Could not load S3 data:", e)
        return

    # --- Step 2: Read Snowflake data ---
    print("🏔️ Reading data from Snowflake...")
    try:
        conn = snowflake.connector.connect(
            user='user',
            password='password',
            account='account',
            warehouse='warehouse',
            database='database',
            schema='schema'
        )
        snowflake_df = pd.read_sql("SELECT * FROM news_tables", conn)
        conn.close()
        print("✅ Successfully loaded Snowflake data:")
        print(snowflake_df.head())
    except Exception as e:
        print("❌ ERROR: Could not connect to Snowflake or run query:", e)
        return

    # --- Step 3: Join datasets ---
    print("🔗 Preparing to join datasets...")

    # Normalize column names to lowercase for consistency
    s3_data.columns = [col.lower() for col in s3_data.columns]
    snowflake_df.columns = [col.lower() for col in snowflake_df.columns]

    try:
        print("S3 columns:", s3_data.columns.tolist())
        print("Snowflake columns:", snowflake_df.columns.tolist())

        # Join on 'url' column
        report_df = pd.merge(s3_data, snowflake_df, on='url', how='inner')

        print("✅ Successfully joined data. Sample:")
        print(report_df.head())
    except KeyError as e:
        print("❌ ERROR: Merge failed. Columns might not exist.")
        print("Details:", e)
        return

    # --- Step 4: Save to file ---
    print("💾 Saving report to /tmp/reporting_file.csv...")
    try:
        report_df.to_csv('/tmp/reporting_file.csv', index=False)
        print("✅ Report saved locally at /tmp/reporting_file.csv")
    except Exception as e:
        print("❌ ERROR: Failed to save CSV:", e)
        return

    # --- Step 5: Upload to S3 ---
    print("☁️ Uploading report to S3 as 'report/final_report.csv'...")
    try:
        s3.upload_file('/tmp/reporting_file.csv', 'varshini-bucket09', 'report/final_report.csv')
        print("✅ Report uploaded to S3 successfully.")
    except Exception as e:
        print("❌ ERROR: Failed to upload file to S3:", e)

# 👇 Entry point
if __name__ == "__main__":
    generate_report()

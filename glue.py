import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Glue job setup
args = getResolvedOptions(sys.argv, ['job-name'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define the schema of your JSON Kafka messages
schema = StructType() \
    .add("title", StringType()) \
    .add("source", StringType()) \
    .add("publishedAt", StringType()) \
    .add("url", StringType())

# Read data from Kafka (skip missing offsets)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker-id") \
    .option("subscribe", "topic") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Convert Kafka values to String and parse JSON
json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# Write the output to S3 in CSV format
output_query = json_df.writeStream \
    .format("csv") \
    .option("path", "s3://bucket/output/") \
    .option("checkpointLocation", "s3://bucket/news-csv/") \
    .outputMode("append") \
    .option("header", "true") \
    .start()

# Block until termination
output_query.awaitTermination()

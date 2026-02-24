import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read Silver table from Data Catalog
silver_df = spark.table("glue_lakehouse_db.silver_sales")

# Create Gold aggregation
gold_df = (
    silver_df
    .groupBy("year")
    .agg(
        F.sum("price").alias("total_revenue"),
        F.count("*").alias("total_transactions")
    )
    .orderBy("year")
)

# Write to S3 as Parquet
gold_df.write \
    .mode("overwrite") \
    .parquet("s3://etl-glue-portfolio-alexandre-2026/analytics/gold_revenue_by_year/")

job.commit()

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, concat_ws, year, month, to_timestamp, to_date


# Job init
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# =========================
# 1️⃣ Read Bronze Tables
# =========================

sales_df = spark.table("glue_lakehouse_db.sales")
customers_df = spark.table("glue_lakehouse_db.customers")

# =========================
# 2️⃣ Transformations
# =========================

# Convert timestamp uma única vez
sales_df = sales_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSSSSS")
)

# Extrair year e month da coluna já convertida
sales_df = sales_df.withColumn("year", year(col("timestamp")))
sales_df = sales_df.withColumn("month", month(col("timestamp")))

# Convert birthday
customers_df = customers_df.withColumn(
    "birthday",
    to_date(col("birthday"))
)

# Create full_name
customers_df = customers_df.withColumn(
    "full_name",
    concat_ws(" ", col("firstname"), col("lastname"))
)

# =========================
# 3️⃣ Join Sales + Customers
# =========================

customers_selected = customers_df.select(
    "customer_id", 
    "full_name",
    "email",
    "address", 
    "birthday", 
    "country"
)

silver_df = sales_df.join(
    customers_selected,
    on="customer_id",
    how="inner"
)

#==========================
#sales_df.select("timestamp", "year", "month").show(10, False)

#==========================

# =========================
# 4️⃣ Write Silver Layer (Parquet + Partition)
# =========================

silver_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("s3://etl-glue-portfolio-alexandre-2026/processed/silver_sales/")

job.commit()


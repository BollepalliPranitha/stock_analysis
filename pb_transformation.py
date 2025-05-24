# Import Libraries
import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, avg, max, min, count, when, row_number, dayofweek, weekofyear, month, quarter, year, expr
from pyspark.sql.window import Window
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
# Initialize Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load staging data from Glue Catalog
company_df = glueContext.create_dynamic_frame.from_catalog(
    database="crypto-db",
    table_name="pb-company"
).toDF()

stocks_df = glueContext.create_dynamic_frame.from_catalog(
    database="crypto-db",
    table_name="pb-stocks"
).toDF()

index_df = glueContext.create_dynamic_frame.from_catalog(
    database="crypto-db",
    table_name="pb-index"
).toDF()

# --- 1. Dim_Date ---
dim_date_df = stocks_df.select("Date").distinct() \
    .withColumn("DayOfWeek", dayofweek("Date")) \
    .withColumn("WeekOfYear", weekofyear("Date")) \
    .withColumn("Month", month("Date")) \
    .withColumn("Quarter", quarter("Date")) \
    .withColumn("Year", year("Date")) 

dim_date_df = dim_date_df.withColumn("DateKey", row_number().over(Window.orderBy("Date")))

# --- 2. Dim_Company ---
dim_company_df = company_df.select(
    col("Symbol"),
    col("Shortname").alias("ShortName"),
    col("Longname").alias("LongName"),
    col("Sector"),
    col("Industry"),
    col("Longbusinesssummary").alias("Longbusinesssummary"),
    col("City"),
    col("State"),
    col("Country")
).distinct().withColumn("CompanyKey", row_number().over(Window.orderBy("Symbol")))

# --- 3. Dim_Exchange ---
dim_exchange_df = spark.createDataFrame([
    (1, "NMS", "United States", "USD"),
    (2, "NYQ", "United States", "USD")
], ["ExchangeKey", "ExchangeName", "Country", "Currency"])

# --- 4. Dim_SP500 ---
dim_sp500_df = index_df.select(
    col("s&p500")
).distinct().withColumn("SP500Key", row_number().over(Window.orderBy("s&p500")))

# --- 5. Fact_Stock_Performance ---
# Join data
fact_df = stocks_df.join(
    company_df, on=["Symbol"], how="inner"
).join(
    index_df, on=["Date"], how="inner"
)

# Add calculated fields
fact_df = fact_df.withColumn(
    "DailyReturn", (col("adj_close#0") - col("Open")) / col("Open")
).withColumn(
    "DailyVolatility", col("High") - col("Low")
).withColumn(
    "VolumeSpike", when(col("Volume") > 1.5 * avg(col("Volume")).over(Window.partitionBy("Symbol")), lit(True)).otherwise(lit(False))
).withColumn(
    "ExcessReturn", col("DailyReturn") - col("s&p500")
).withColumn(
    "MA5", avg("adj_close#0").over(Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-4, 0))
).withColumn(
    "MA20", avg("adj_close#0").over(Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-19, 0))
).withColumn(
    "MA50", avg("adj_close#0").over(Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-49, 0))
)

fact_df = fact_df.select(
    row_number().over(Window.orderBy("Date")).alias("StockPerfKey"),
    col("Date"),
    col("Symbol").alias("StockSymbol"),
    col("adj_close#0").alias("AdjustedClose"),
    col("Close"),
    col("High"),
    col("Low"),
    col("Open"),
    col("Volume"),
    col("DailyReturn"),
    col("DailyVolatility"),
    col("VolumeSpike"),
    col("ExcessReturn"),
    col("MA5"),
    col("MA20"),
    col("MA50"),
    col("CurrentPrice"),
    col("MarketCap"),
    col("EBITDA"),
    col("RevenueGrowth"),
    col("FullTimeEmployees"),
    col("Weight")
)

# --- Write to Snowflake ---
# Snowflake connection details
snowflake_options = {
    "sfURL": "",
    "sfDatabase": "",
    "sfSchema": "",
    "sfWarehouse": "",
    "sfRole": "",
    "sfUser": "",
    "sfPassword": ""
}

# Write Dim_Date
dim_date_df.write.format("snowflake").options(**snowflake_options).option("dbtable", "DIM_DATE").save()

# Write Dim_Company
dim_company_df.write.format("snowflake").options(**snowflake_options).option("dbtable", "DIM_COMPANY").save()

# Write Dim_Exchange
dim_exchange_df.write.format("snowflake").options(**snowflake_options).option("dbtable", "DIM_EXCHANGE").save()

# Write Dim_SP500
dim_sp500_df.write.format("snowflake").options(**snowflake_options).option("dbtable", "DIM_SP500").save()

# Write Fact Table
fact_df.write.format("snowflake").options(**snowflake_options).option("dbtable", "FACT_STOCK_PERFORMANCE").save()

# Commit job
job.commit()

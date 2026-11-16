# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from datetime import datetime
import uuid
from pyspark.sql.types import (
    StructType, StructField, StringType,
    TimestampType, DoubleType, LongType
)

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS usgs_lakehouse.pipeline_audit_log")

# COMMAND ----------

audit_schema = StructType([
    StructField("pipeline_name", StringType(), True),
    StructField("pipeline_run_id", StringType(), True),
    StructField("layer_name", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("records_read", LongType(), True),
    StructField("records_written", LongType(), True),
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True)
])

# COMMAND ----------

# Create Logging Function

def write_audit_log(
    pipeline_name,
    pipeline_run_id,
    layer_name,
    table_name,
    start_time,
    end_time,
    records_read,
    records_written,
    status,
    error_message
):
    duration_seconds = (end_time - start_time).total_seconds()

    audit_record = [(
        pipeline_name,
        pipeline_run_id,
        layer_name,
        table_name,
        start_time,
        end_time,
        duration_seconds,
        records_read,
        records_written,
        status,
        error_message
    )]

    audit_df = spark.createDataFrame(audit_record, schema=audit_schema)

    audit_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("usgs_lakehouse.pipeline_audit_log")



# COMMAND ----------

# Sliver Table Log

pipeline_run_id = str(uuid.uuid4())
start_time = datetime.now()

try:
    silver_count = spark.read.table("usgs_lakehouse.silver_earthquakes").count()

    end_time = datetime.now()

    write_audit_log(
        pipeline_name="USGS_Earthquake_Pipeline",
        pipeline_run_id=pipeline_run_id,
        layer_name="Silver",
        table_name="silver_earthquakes",
        start_time=start_time,
        end_time=end_time,
        records_read=silver_count,
        records_written=silver_count,
        status="SUCCESS",
        error_message=None
    )

except Exception as e:
    end_time = datetime.now()

    write_audit_log(
        pipeline_name="USGS_Earthquake_Pipeline",
        pipeline_run_id=pipeline_run_id,
        layer_name="Silver",
        table_name="silver_earthquakes",
        start_time=start_time,
        end_time=end_time,
        records_read=0,
        records_written=0,
        status="FAILED",
        error_message=str(e)
    )


# COMMAND ----------

display(
    spark.sql("""
    SELECT *
    FROM usgs_lakehouse.pipeline_audit_log
    ORDER BY start_time DESC
    """)
)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


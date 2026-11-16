# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Project Description
# MAGIC
# MAGIC In this project, we build an end-to-end Data Engineering pipeline using the USGS (United States Geological Survey) Earthquake API and Azure Databricks.
# MAGIC
# MAGIC The pipeline ingests live earthquake data from the USGS REST API, stores the raw JSON data in a Bronze Delta table, transforms and cleans the data into a structured Silver layer, and creates analytical Gold tables for reporting and visualization.
# MAGIC
# MAGIC The pipeline is designed using the Medallion Architecture (Bronze → Silver → Gold) and follows enterprise Data Engineering best practices.
# MAGIC
# MAGIC To improve efficiency, the pipeline implements incremental loading using a watermark based on the latest earthquake event time. During each execution, the pipeline retrieves only earthquake events that occurred after the most recently processed event, reducing unnecessary API calls and duplicate processing.
# MAGIC
# MAGIC Delta Lake MERGE operations are used to perform UPSERT (Update + Insert) operations, ensuring that duplicate earthquake records are not created while allowing existing records to be updated if changes occur.
# MAGIC
# MAGIC The project also includes data quality validation, audit logging, and pipeline monitoring to simulate a production-ready data engineering solution.

# COMMAND ----------

# MAGIC %md
# MAGIC ```text
# MAGIC Pipeline Starts
# MAGIC         │
# MAGIC         ▼
# MAGIC Does Bronze Table Exist?
# MAGIC         │
# MAGIC    ┌────┴────┐
# MAGIC    │         │
# MAGIC   No        Yes
# MAGIC    │         │
# MAGIC Full Load   Read Latest Event Time
# MAGIC (Default)   from Silver Layer
# MAGIC    │         │
# MAGIC    └────┬────┘
# MAGIC         ▼
# MAGIC Build Dynamic API URL
# MAGIC         │
# MAGIC         ▼
# MAGIC Call USGS REST API
# MAGIC         │
# MAGIC         ▼
# MAGIC Retrieve Only New Earthquakes
# MAGIC         │
# MAGIC         ▼
# MAGIC Create Bronze DataFrame
# MAGIC         │
# MAGIC         ▼
# MAGIC MERGE into Bronze Delta Table
# MAGIC         │
# MAGIC         ▼
# MAGIC Verification
# MAGIC         │
# MAGIC         ▼
# MAGIC Pipeline Complete
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Library 

# COMMAND ----------

import requests
import json
import time

from datetime import datetime , timedelta
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run "/Users/officialmanishchhetri@gmail.com/USGS_Earthquake_Project/common/common_utils"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration 

# COMMAND ----------

# MAGIC %run "/Users/officialmanishchhetri@gmail.com/USGS_Earthquake_Project/common/config"

# COMMAND ----------

# MAGIC %md
# MAGIC # Incremental Loading
# MAGIC
# MAGIC Before calling the USGS API, the pipeline first determines whether the Bronze Delta table already exists.
# MAGIC
# MAGIC If the Bronze table does not exist, the pipeline performs a Full Load by retrieving earthquake data from the configured default start date.
# MAGIC
# MAGIC If the Bronze table already exists, the pipeline reads the latest processed earthquake event time (watermark) from the brozen table.
# MAGIC
# MAGIC The retrieved watermark is then used as the `starttime` parameter in the USGS API request. As a result, the API returns only earthquake events that occurred after the latest successfully processed earthquake.
# MAGIC
# MAGIC This incremental loading strategy significantly reduces API calls, minimizes data movement, avoids duplicate processing, and improves overall pipeline performance while maintaining data accuracy.

# COMMAND ----------

table_exists = spark.catalog.tableExists(BRONZE_TABLE)

log(f"Bronze Table Exists : {table_exists}")

if table_exists:

    latest_event_ms = (
    spark.table(BRONZE_TABLE)
    .selectExpr("max(cast(get_json_object(properties, '$.time') as long)) as latest_event_ms")
    .collect()[0]["latest_event_ms"]
    )

    latest_event = datetime.utcfromtimestamp(latest_event_ms / 1000)
    
    print("Latest Earthquake Event Time:", latest_event)


else:

    latest_event = None

    log("First Pipeline Execution. Performing Full Load.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build the Dynamic API URL

# COMMAND ----------

# (https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson)

if latest_event is None:

    usgs_api_url = (
        f"{USGS_BASE_URL}"
        f"?format={API_FORMAT}"
        f"&starttime={DEFAULT_START_DATE}"
        f"&orderby={API_ORDER_BY}"
    )

else:

    start_time = latest_event.strftime("%Y-%m-%dT%H:%M:%S")
    end_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    usgs_api_url = (
        f"{USGS_BASE_URL}"
        f"?format={API_FORMAT}"
        f"&starttime={start_time}"
        f"&endtime={end_time}"
        f"&orderby={API_ORDER_BY}"
    )

print(usgs_api_url)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch API

# COMMAND ----------

# Fetching Data from API


response = None

for attempt in range(1, MAX_RETRIES + 1):

    try:
        log(f"API attempt {attempt} of {MAX_RETRIES}")

        response = requests.get(
            usgs_api_url,
            timeout=REQUEST_TIMEOUT_SECONDS
        )

        if response.status_code == 200:
            log("API request successful")
            break
        else:
            log(f"API returned status code: {response.status_code}")

    except requests.exceptions.Timeout:
        log("API request timed out")

    except requests.exceptions.RequestException as e:
        log(f"API request failed: {e}")

    if attempt < MAX_RETRIES:
        log("Retrying in 5 seconds...")
        time.sleep(5)

if response is None or response.status_code != 200:
    raise Exception("USGS API request failed after all retry attempts")


# print("Status Code:", response.status_code)

data = response.json()
  

# COMMAND ----------

# Validate JSON Response

if "features" not in data:
    raise Exception("Invalid API response: 'features' key is missing")

if "metadata" not in data:
    raise Exception("Invalid API response: 'metadata' key is missing")


earthquake_records = data["features"]



if not isinstance(earthquake_records, list):
    raise Exception("Invalid API response: 'features' is not a list")

record_count = len(earthquake_records)

print(f"Number of earthquake records received: {record_count}")


# COMMAND ----------


# Handle Zero New Records


if record_count == 0:
    log("No new earthquake records found.")
    log("Skipping Bronze DataFrame creation and MERGE.")

    dbutils.notebook.exit("No new records to process")

# COMMAND ----------

# print(data)
# print(data['type'])
# print(data['metadata'])
# print(data['features'])
# print(data['features'][0])
# print(data['features'][0]['properties'])
# print(data['features'][0]['geometry'])

# COMMAND ----------

# NUMBER OF EARTHQUAKES

earthquake_records = data["features"]

print("Number of earthquake records:", len(earthquake_records))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creat Bronze Records

# COMMAND ----------

bronze_records = []

for record in earthquake_records:
    bronze_records.append({
        "id": record["id"],
        "type": record["type"],
        "properties": json.dumps(record["properties"]),
        "geometry": json.dumps(record["geometry"]),
        "source_system": "USGS",
        "api_endpoint": usgs_api_url,
        "load_type": "FULL_LOAD"
    })

bronze_df = spark.createDataFrame(bronze_records)

bronze_df = bronze_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("load_date", current_date())


# display(bronze_df)
# bronze_df.show(5, truncate=False)

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Spark DataFrame

# COMMAND ----------

bronze_df = spark.createDataFrame(bronze_records)

bronze_df = bronze_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("load_date", current_date())

display(bronze_df)

# COMMAND ----------

# Remove rows with NULL earthquake IDs , 

total_records = bronze_df.count()

bronze_df = bronze_df.filter(
    col("id").isNotNull() &
    col("properties").isNotNull() &
    col("geometry").isNotNull()
)

valid_records = bronze_df.count()

rejected_records = total_records - valid_records

print(f"Total Records: {total_records}")
print(f"Valid Records: {valid_records}")
print(f"Rejected Records: {rejected_records}")

# COMMAND ----------

# Bronze Table
if not table_exists:

    log("Creating Bronze Table...")

    bronze_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(BRONZE_TABLE)

    log("Bronze Table Created")

else:

    log("Incremental Load")

    bronze_delta = DeltaTable.forName(
        spark,
        BRONZE_TABLE
    )

    (
        bronze_delta.alias("target")
        .merge(
            bronze_df.alias("source"),
            "target.id = source.id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    log("Merge Completed")

# COMMAND ----------

# Post-MERGE Record Count Validation


bronze_total_count = spark.table(BRONZE_TABLE).count()

duplicate_count = spark.sql(f"""
    SELECT id, COUNT(*) AS record_count
    FROM {BRONZE_TABLE}
    GROUP BY id
    HAVING COUNT(*) > 1
""").count()

print("Bronze total records:", bronze_total_count)
print("Duplicate earthquake IDs:", duplicate_count)

if duplicate_count > 0:
    raise Exception("Data quality failed: duplicate earthquake IDs found in Bronze table")
else:
    log("Record count validation passed. No duplicate earthquake IDs found.")

# COMMAND ----------

# Check Latest event Watermark
spark.sql("""
SELECT
    MAX(CAST(get_json_object(properties, '$.time') AS BIGINT)) AS latest_event_time_ms
FROM usgs_lakehouse.bronze_earthquakes
""").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     max(cast(get_json_object(properties,'$.time') AS BIGINT))
# MAGIC FROM usgs_lakehouse.bronze_earthquakes

# COMMAND ----------

# Check latest loaded records

spark.sql("""
SELECT
    id,
    get_json_object(properties, '$.title') AS title,
    ingestion_timestamp,
    load_date
FROM usgs_lakehouse.bronze_earthquakes
ORDER BY ingestion_timestamp DESC
LIMIT 10
""").show(truncate=False)


# COMMAND ----------

# # Bronze Data Quality 

# null_id_count = spark.sql(f"""
#     SELECT COUNT(*) AS null_id_count
#     FROM {BRONZE_TABLE}
#     WHERE id IS NULL
# """).collect()[0]["null_id_count"]

# invalid_properties_count = spark.sql(f"""
#     SELECT COUNT(*) AS invalid_properties_count
#     FROM {BRONZE_TABLE}
#     WHERE properties IS NULL
# """).collect()[0]["invalid_properties_count"]

# invalid_geometry_count = spark.sql(f"""
#     SELECT COUNT(*) AS invalid_geometry_count
#     FROM {BRONZE_TABLE}
#     WHERE geometry IS NULL
# """).collect()[0]["invalid_geometry_count"]

# print("Null ID Count:", null_id_count)
# print("Invalid Properties Count:", invalid_properties_count)
# print("Invalid Geometry Count:", invalid_geometry_count)

# if null_id_count > 0:
#     raise Exception("Data quality failed: null earthquake IDs found")

# if invalid_properties_count > 0:
#     raise Exception("Data quality failed: null properties JSON found")

# if invalid_geometry_count > 0:
#     raise Exception("Data quality failed: null geometry JSON found")

# print("Bronze data quality checks passed.")

# COMMAND ----------

spark.sql(f"""
SELECT COUNT(*)
FROM {BRONZE_TABLE}
""").show()

# spark.table(BRONZE_TABLE).count()
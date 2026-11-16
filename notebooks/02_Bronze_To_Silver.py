# Databricks notebook source
# MAGIC %md
# MAGIC ### Library

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    IntegerType,
    ArrayType
)
from pyspark.sql.functions import from_json, col

# COMMAND ----------


# Properties schema

properties_schema = StructType([
    StructField("mag", DoubleType(), True),
    StructField("place", StringType(), True),
    StructField("time", LongType(), True),
    StructField("updated", LongType(), True),
    StructField("tz", StringType(), True),
    StructField("url", StringType(), True),
    StructField("detail", StringType(), True),
    StructField("felt", IntegerType(), True),
    StructField("cdi", DoubleType(), True),
    StructField("mmi", DoubleType(), True),
    StructField("alert", StringType(), True),
    StructField("status", StringType(), True),
    StructField("tsunami", IntegerType(), True),
    StructField("sig", IntegerType(), True),
    StructField("net", StringType(), True),
    StructField("code", StringType(), True),
    StructField("ids", StringType(), True),
    StructField("sources", StringType(), True),
    StructField("types", StringType(), True),
    StructField("nst", IntegerType(), True),
    StructField("dmin", DoubleType(), True),
    StructField("rms", DoubleType(), True),
    StructField("gap", DoubleType(), True),
    StructField("magType", StringType(), True),
    StructField("type", StringType(), True),
    StructField("title", StringType(), True)
])

# COMMAND ----------

# Geometry schema

geometry_schema = StructType([
    StructField("type", StringType(), True),
    StructField("coordinates", ArrayType(DoubleType()), True)
])
# # Bronze schema

# bronze_schema = StructType([
#     StructField("api_endpoint", StringType(), True),
#     StructField("geometry", StringType(), True),
#     StructField("id", StringType(), True),
#     StructField("load_type", StringType(), True),
#     StructField("properties", StringType(), True),
#     StructField("source_system", StringType(), True),
#     StructField("type", StringType(), True),
#     StructField("ingestion_timestamp", TimestampType(), True),
#     StructField("load_date", DateType(), True)
# ])
# # Read bronze table
# bronze_df = spark.read.format("delta").load("/mnt/workshop/curated/earthquake/bronze")
# # Parse geometry column
# bronze_df = bronze_df.withColumn("geometry", from_json(bronze_df.geometry, geometry_schema))
# # Parse properties column
# bronze_df = bronze_df.withColumn("properties", from_json(bronze_df.properties, properties_schema))
# # Drop columns
# bronze_df = bronze_df.drop("geometry.coordinates", "properties.magType", "properties.type", "properties.title")
# # Rename columns
# bronze_df = bronze_df.withColumnRenamed("properties.mag", "mag").withColumnRenamed("properties.place", "place").withColumnRenamed("properties.time", "time").withColumnRenamed("properties.updated", "updated").withColumnRenamed("properties.tz", "tz").withColumnRenamed("properties.url", "url").withColumnRenamed("properties.detail", "detail").withColumnRenamed("properties.felt", "felt").withColumnRenamed("properties.cdi", "cdi").withColumnRenamed("properties.mmi", "mmi").withColumnRenamed("properties.alert", "alert").withColumnRenamed("properties.status", "status").withColumnRenamed("properties.net", "net").withColumnRenamed("properties.code", "code").withColumnRenamed("properties.ids", "ids").withColumnRenamed("properties.sources", "sources").withColumnRenamed("properties.types", "types").withColumnRenamed("properties.nst", "nst").withColumnRenamed("properties.dmin", "dmin").withColumnRenamed("properties.rms", "rms").withColumnRenamed("properties.gap", "gap").withColumnRenamed("properties.sig", "sig").withColumnRenamed("properties.tsunami", "tsunami")
# # Explode coordinates column
# bronze_df = bronze_df.withColumn("coordinates", explode(bronze_df.geometry.coordinates))
# # Drop columns
# bronze_df = bronze_df.drop("geometry", "properties")
# # Rename columns
# bronze_df = bronze_df.withColumnRenamed("coordinates", "longitude").withColumnRenamed("longitude", "latitude")
# # Write bronze table
# bronze_df.write.format("delta").mode("overwrite").save("/mnt/workshop/curated/earthquake/bronze")   

# COMMAND ----------

bronze_df = spark.read.table("usgs_lakehouse.bronze_earthquakes")

parsed_df = bronze_df \
    .withColumn("properties_parsed", from_json(col("properties"), properties_schema)) \
    .withColumn("geometry_parsed", from_json(col("geometry"), geometry_schema))

display(parsed_df)

# COMMAND ----------

# Flattening the nested columns

silver_df = parsed_df.select(

    col("id").alias("earthquake_id"),

    col("properties_parsed.mag").alias("magnitude"),
    col("properties_parsed.place").alias("place"),
    col("properties_parsed.time").alias("event_time_ms"),
    col("properties_parsed.updated").alias("updated_time_ms"),
    col("properties_parsed.tsunami").alias("tsunami"),
    col("properties_parsed.alert").alias("alert"),
    col("properties_parsed.sig").alias("significance"),
    col("properties_parsed.status").alias("status"),
    col("properties_parsed.net").alias("network"),
    col("properties_parsed.magType").alias("magnitude_type"),
    col("properties_parsed.type").alias("event_type"),
    col("properties_parsed.title").alias("title"),

    col("geometry_parsed.coordinates")[0].alias("longitude"),
    col("geometry_parsed.coordinates")[1].alias("latitude"),
    col("geometry_parsed.coordinates")[2].alias("depth_km"),

    col("source_system"),
    col("api_endpoint"),
    col("load_type"),
    col("ingestion_timestamp"),
    col("load_date")
        
)

# COMMAND ----------

display(silver_df)

# COMMAND ----------

from pyspark.sql.functions import from_unixtime


silver_df = silver_df \
    .withColumn( "event_time", from_unixtime(col("event_time_ms") / 1000).cast("timestamp") ) \
    .withColumn( "updated_time", from_unixtime(col("updated_time_ms") / 1000).cast("timestamp") ) 
         


# COMMAND ----------

silver_df.select(
    "event_time_ms",
    "event_time",
    "updated_time_ms",
    "updated_time"
).show(5, truncate=False)

# COMMAND ----------

#  Check total records
print("Total records:", silver_df.count())



# COMMAND ----------

# Check duplicate earthquake IDs
duplicate_df = silver_df.groupBy("earthquake_id").count().filter("count > 1")
# display(duplicate_df)

# COMMAND ----------

#  Check nulls in important columns
from pyspark.sql.functions import col, count, when

null_check_df = silver_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in ["earthquake_id", "magnitude", "place", "event_time", "longitude", "latitude", "depth_km"]
])
# display(null_check_df)

# COMMAND ----------


# Check invalid coordinates
invalid_coordinates_df = silver_df.filter(
    (col("latitude") < -90) | (col("latitude") > 90) |
    (col("longitude") < -180) | (col("longitude") > 180)
)

# display(invalid_coordinates_df)

# COMMAND ----------

# Saving the data in the silver table
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("usgs_lakehouse.silver_earthquakes")

# COMMAND ----------

display(
    spark.sql("""
    SELECT *
    FROM usgs_lakehouse.silver_earthquakes
    LIMIT 10
    """)
)

# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS total_records
FROM usgs_lakehouse.silver_earthquakes
""").show()

# COMMAND ----------

spark.sql("""
DESCRIBE usgs_lakehouse.silver_earthquakes
""").show(truncate=False)
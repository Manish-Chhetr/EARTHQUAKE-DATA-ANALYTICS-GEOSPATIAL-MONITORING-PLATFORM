# Databricks notebook source
from pyspark.sql.functions import (
    col,
    count,
    avg,
    max,
    min,
    to_date,
    year,
    month,
    hour,
    when
)

silver_df = spark.read.table("usgs_lakehouse.silver_earthquakes")

# display(silver_df)

# COMMAND ----------

# Daily Earthquake Summary 

daily_summary_df = silver_df.groupBy(
    to_date(col("event_time")).alias("event_date")
).agg(
    count("*").alias("total_earthquakes"),
    avg("magnitude").alias("avg_magnitude"),
    max("magnitude").alias("max_magnitude"),
    min("magnitude").alias("min_magnitude"),
    avg("depth_km").alias("avg_depth_km")
)

display(daily_summary_df)



# COMMAND ----------

daily_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("usgs_lakehouse.gold_daily_earthquake_summary")


# COMMAND ----------

# Magnitude Distribution

magnitude_distribution_df = silver_df.withColumn(
    "magnitude_category",
    when(col("magnitude") < 1, "Micro")
    .when((col("magnitude") >= 1) & (col("magnitude") < 2), "Minor")
    .when((col("magnitude") >= 2) & (col("magnitude") < 3), "Light")
    .when((col("magnitude") >= 3) & (col("magnitude") < 4), "Moderate")
    .when((col("magnitude") >= 4) & (col("magnitude") < 5), "Strong")
    .otherwise("Major")
).groupBy("magnitude_category").agg(
    count("*").alias("earthquake_count")
)

display(magnitude_distribution_df)

# COMMAND ----------

magnitude_distribution_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("usgs_lakehouse.gold_magnitude_distribution")

# COMMAND ----------

# Tsunami Summary

tsunami_summary_df = silver_df.groupBy("tsunami").agg(
    count("*").alias("earthquake_count")
)

display(tsunami_summary_df)

# COMMAND ----------

tsunami_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("usgs_lakehouse.gold_tsunami_summary")

# COMMAND ----------

# Region / Location Table

region_summary_df = silver_df.groupBy(
    "place",
    "latitude",
    "longitude"
).agg(
    count("*").alias("earthquake_count"),
    avg("magnitude").alias("avg_magnitude"),
    max("magnitude").alias("max_magnitude"),
    min("magnitude").alias("min_magnitude"),
    avg("depth_km").alias("avg_depth_km")
)

display(region_summary_df)


# COMMAND ----------

region_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("usgs_lakehouse.gold_region_summary")

# COMMAND ----------

spark.sql("""
SELECT *
FROM usgs_lakehouse.gold_region_summary
ORDER BY earthquake_count DESC
LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# Monthly Trend

monthly_trend_df = silver_df.groupBy(
    year(col("event_time")).alias("event_year"),
    month(col("event_time")).alias("event_month")
).agg(
    count("*").alias("total_earthquakes"),
    avg("magnitude").alias("avg_magnitude"),
    max("magnitude").alias("max_magnitude"),
    avg("depth_km").alias("avg_depth_km")
).orderBy("event_year", "event_month")

# COMMAND ----------

monthly_trend_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("usgs_lakehouse.gold_monthly_trend")

# COMMAND ----------

display(
    spark.sql("""
    SELECT *
    FROM usgs_lakehouse.gold_monthly_trend
    ORDER BY event_year, event_month
    """)
)

# COMMAND ----------

# Hourly Trend

hourly_trend_df = silver_df.groupBy(
    hour(col("event_time")).alias("event_hour")
).agg(
    count("*").alias("total_earthquakes"),
    avg("magnitude").alias("avg_magnitude"),
    max("magnitude").alias("max_magnitude")
).orderBy("event_hour")

display(hourly_trend_df)

# COMMAND ----------

hourly_trend_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("usgs_lakehouse.gold_hourly_trend")

# COMMAND ----------

display(
    spark.sql("""
    SELECT *
    FROM usgs_lakehouse.gold_hourly_trend
    ORDER BY event_hour
    """)
)

# COMMAND ----------

spark.sql("SHOW TABLES IN usgs_lakehouse").show(truncate=False)

# COMMAND ----------

display(spark.sql("""
SELECT *
FROM usgs_lakehouse.gold_daily_earthquake_summary
"""))

# COMMAND ----------

tables_to_export = [
    "gold_daily_earthquake_summary",
    "gold_magnitude_distribution",
    "gold_tsunami_summary",
    "gold_region_summary",
    "gold_monthly_trend",
    "gold_hourly_trend"
]

for table_name in tables_to_export:

    df = spark.table(f"usgs_lakehouse.{table_name}")

    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(f"/tmp/{table_name}")

    print(f"{table_name} exported.")

# COMMAND ----------

display(
    spark.table("usgs_lakehouse.gold_daily_earthquake_summary")
)

# COMMAND ----------

gold_daily_df = spark.table("usgs_lakehouse.gold_daily_earthquake_summary")

pdf = gold_daily_df.toPandas()

pdf.to_csv("gold_daily_earthquake_summary.csv", index=False)

# COMMAND ----------


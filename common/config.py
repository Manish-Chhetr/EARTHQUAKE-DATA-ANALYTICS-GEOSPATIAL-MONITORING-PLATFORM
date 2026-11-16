# Databricks notebook source

#   Configuration


DATABASE_NAME = "usgs_lakehouse"

BRONZE_TABLE = f"{DATABASE_NAME}.bronze_earthquakes"
SILVER_TABLE = f"{DATABASE_NAME}.silver_earthquakes"
GOLD_TABLE = f"{DATABASE_NAME}.gold_earthquakes"

USGS_BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

DEFAULT_START_DATE = "2026-01-01"

API_FORMAT = "geojson"
API_ORDER_BY = "time"

REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 5
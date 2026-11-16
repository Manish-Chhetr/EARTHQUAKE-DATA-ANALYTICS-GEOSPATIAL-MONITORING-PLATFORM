# Databricks notebook source
# Logging function

from datetime import datetime


def log(message):
    print(f"[INFO] {datetime.now()} : {message}")
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import os
import pyspark.sql.functions as F
import pyspark
from delta import *

DATALAKE_PATH = '/home/leolp/Área de trabalho/Portfolio/datalake'

builder = pyspark.sql.SparkSession.builder.appName("Projeto_1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
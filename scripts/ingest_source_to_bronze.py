from datetime import datetime, timedelta
from delta import *
from pathlib import Path
import os
import pandas as pd
import pyspark
import pyspark.sql.functions as F

builder = pyspark.sql.SparkSession.builder.appName("Projeto_1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

DATALAKE_PATH = '/home/leolp/Área de trabalho/Portfolio/datalake'

def ingest_source_to_bronze(data_processamento:str) -> None:

    SOURCE_PATH = f"{DATALAKE_PATH}/source/transaction_system/transaction_data_{data_processamento.replace('-', '_')}.csv"
    BRONZE_PATH = f"{DATALAKE_PATH}/bronze/transaction_data"

    df_source = (
        spark
        .read
        .option('header', 'true')
        .option('inferSchema', 'true')
        .csv(SOURCE_PATH)
    )

    try:
        df_bronze = DeltaTable.forPath(spark, BRONZE_PATH)
        df_bronze.toDF().limit(1)

        # Para remover duplicidade e nao ficar appendando o mesmo dado
        df_bronze.delete(f"transaction_date = '{data_processamento}'")

        (
            df_source
            .write
            .format('delta')
            .mode('append')
            .option('mergeSchema', 'true')
            .save(BRONZE_PATH)
        )

    except Exception as e:
        if "DELTA_MISSING_DELTA_TABLE" in str(e):
            (
                df_source
                .write
                .format('delta')
                .option('mergeSchema', 'true')
                .mode('overwrite')
                .save(BRONZE_PATH)
            )
        else:
            raise e

    return BRONZE_PATH
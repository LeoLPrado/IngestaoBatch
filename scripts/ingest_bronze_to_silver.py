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

def ingest_bronze_to_silver() -> None:
    BRONZE_PATH = f"{DATALAKE_PATH}/bronze/transaction_data"
    SILVER_PATH = f"{DATALAKE_PATH}/silver/transaction_data"

    df_bronze = DeltaTable.forPath(spark, BRONZE_PATH)

    try:
        df_silver = DeltaTable.forPath(spark, SILVER_PATH)
        df_silver.toDF().limit(1)

        # Pegando a maior data da nossa silver e pegar apenas o dados da bronze que vem depois dessa data
        MAIOR_DATA_SILVER = (
            df_silver
            .toDF()
            .agg(
                F.max('transaction_date')
            )
            .collect()[0][0]
        )

        df_bronze = df_bronze.toDF().filter(f"transaction_date >= '{MAIOR_DATA_SILVER}'")

        # Fazendo o Merge
        (
            df_silver.alias('old_data')
            .merge(
                df_bronze.alias('new_data'),
                "old_data.transaction_id = new_data.transaction_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    except Exception as e:
        if 'DELTA_MISSING_DELTA_TABLE' in str(e):
            (
                df_bronze
                .toDF()
                .write
                .format('delta')
                .option('mergeSchema', 'true')
                .mode('overwrite')
                .save(SILVER_PATH)
            )
        else:
            raise e
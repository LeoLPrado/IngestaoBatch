from variable_utils import *

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
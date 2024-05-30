import constants
import time
import logging
import datetime

from pathlib import Path
from typing import List
from pyspark.sql import SparkSession, functions

UNNAMED_COL_NMI = "_c0"
UNNAMED_COL_INTERVAL_DATE = "_c3"

COL_NMI = "nmi"
COL_TIMESTAMP = "timestamp"
COL_CONSUMPTION = "consumption"

def generate_interval_value_cols() -> List[str]:
    """
    For interval records using 30-min intervals, we will have 48 cols
    """
    return [f"_c{i}" for i in range(4,23)]

def output_file_name(fp: str) -> str:
    return f"{constants.DEFAULT_OUTPUT_DIR}/{Path(fp).stem}.output"

def process_nem12_300(nem300_file: str, output_format: str, log_level: str, hold_ui: bool):
    """
    Runs a spark job to process NEM12_300 Records
    """
    logging.info("starting spark job")

    start = time.time()
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("nem12 consumption") \
        .getOrCreate()
    spark.sparkContext.setLogLevel(log_level)

    df = spark.read.csv(nem300_file, inferSchema=True)
    df_sum = df.withColumn(
        COL_CONSUMPTION, functions.expr('+'.join(generate_interval_value_cols())),
    )

    # (NMI, timestamp) -> consumption
    df_sum = df_sum \
        .groupBy(UNNAMED_COL_NMI, UNNAMED_COL_INTERVAL_DATE) \
        .sum(COL_CONSUMPTION) \
        .withColumnRenamed("sum(consumption)", COL_CONSUMPTION) \
        .withColumnRenamed(UNNAMED_COL_NMI, COL_NMI) \
        .withColumnRenamed(UNNAMED_COL_INTERVAL_DATE, COL_TIMESTAMP)

    if output_format == constants.OUTPUT_FMT_CSV:
        df_sum.write.csv(output_file_name(nem300_file), header=True)
    else:
        df_sql = df_sum.withColumn(
            "sql",
            functions.format_string(f"INSERT INTO {constants.DEFAULT_SQL_TABLE_NAME}({COL_NMI}, {COL_TIMESTAMP}, {COL_CONSUMPTION}) VALUES ('%s', %s, %s) ON CONFLICT ({COL_NMI}, {COL_TIMESTAMP}) DO UPDATE SET {COL_CONSUMPTION} = %s", "nmi", "timestamp", "consumption", "consumption"),
        )
        df_sql.select("sql").write.format("text").save(output_file_name(nem300_file))

    end = time.time()
    logging.info(f"spark job completed in {str(datetime.timedelta(seconds=end - start))}s")

    if hold_ui:
        input()
        spark.stop()


import constants

from pathlib import Path
from typing import List
from pyspark.sql import SparkSession, functions

UNNAMED_COL_NMI = "_c0"
UNNAMED_COL_INTERVAL_DATE = "_c3"

DEFAULT_COL_NMI = "nmi"
DEFAULT_COL_TIMESTAMP = "timestamp"
DEFAULT_COL_CONSUMPTION = "consumption"

def generate_interval_value_cols() -> List[str]:
    """
    For interval records using 30-min intervals, we will have 48 cols
    """
    return [f"_c{i}" for i in range(4,49)]

def output_file_name(fp: str) -> str:
    return f"{constants.DEFAULT_OUTPUT_DIR}/{Path(fp).stem}.output"

def process_nem300(nem300_file: str, output_format: str, coalesce: bool, log_level: str):

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("nem12 consumption") \
        .getOrCreate()
    spark.sparkContext.setLogLevel(log_level)

    df = spark.read.csv(nem300_file, inferSchema=True)
    df_sum = df.withColumn(
        DEFAULT_COL_CONSUMPTION,
        functions.expr('+'.join(generate_interval_value_cols())),
    )

    # (NMI, timestamp) -> consumption
    df_sum = df_sum \
        .groupBy(UNNAMED_COL_NMI, UNNAMED_COL_INTERVAL_DATE) \
        .sum(DEFAULT_COL_CONSUMPTION) \
        .withColumnRenamed("sum(consumption)", DEFAULT_COL_CONSUMPTION) \
        .withColumnRenamed(UNNAMED_COL_NMI, DEFAULT_COL_NMI) \
        .withColumnRenamed(UNNAMED_COL_INTERVAL_DATE, DEFAULT_COL_TIMESTAMP)

    if output_format == constants.OUTPUT_FMT_CSV:
        if coalesce:
            df_sum.coalesce(1).write.csv(output_file_name(nem300_file), header=True)
        else:
            df_sum.write.csv(output_file_name(nem300_file), header=True)
            return

    df_sql = df_sum.withColumn(
        "sql",
        functions.format_string(f"INSERT INTO {constants.DEFAULT_SQL_TABLE_NAME}({DEFAULT_COL_NMI}, {DEFAULT_COL_TIMESTAMP}, {DEFAULT_COL_CONSUMPTION}) VALUES ('%s', %s, %s)", "nmi", "timestamp", "consumption"),
    )

    if coalesce:
        df_sql.coalesce(1).select("sql").write.format("text").save(output_file_name(nem300_file))
    else:
        df_sql.select("sql").write.format("text").save(output_file_name(nem300_file))


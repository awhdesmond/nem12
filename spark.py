import constants
import time
import json
import os
import logging
import time
import datetime

from pathlib import Path
from typing import List
from pyspark.sql import SparkSession, functions

UNNAMED_COL_NMI = "_c0"
UNNAMED_COL_INTERVAL_DATE = "_c3"

COL_NMI = "nmi"
COL_TIMESTAMP = "timestamp"
COL_CONSUMPTION = "consumption"

class NEM12Transformer:

    def __init__(self):
        self.num_nmi_date_set = set()

    def _is_non_200_300(self, line: str) -> bool:
        return not (line.startswith(constants.NME12_200) or line.startswith(constants.NME12_300))

    def _make_output_folder(self):
        os.makedirs(constants.DEFAULT_OUTPUT_DIR, exist_ok=True)

    def _temp_output_fp(self, fp: str) -> str:
        return f"{constants.DEFAULT_OUTPUT_DIR}/{Path(fp).stem}.transformed.csv"

    def _statistics_fp(self, fp: str) -> str:
        return f"{constants.DEFAULT_OUTPUT_DIR}/{Path(fp).stem}.stats.json"

    def augment_nem12_300_with_nmi(self, interval_data: str, nmi: str, interval_len: int):
        return ",".join([nmi, interval_len, interval_data])

    def generate_statistics(self, fp: str):
        stats_file = self._statistics_fp(fp)
        logging.info(f"generating statistics at {stats_file}")

        with open(stats_file, "w+") as outfile:
            json.dump(dict(num_nmi_date_pairs=len(self.num_nmi_date_set)), outfile)

    def process(self, fp: str) ->  str :
        self._make_output_folder()
        output_fp = self._temp_output_fp(fp)

        logging.info(f"transforming NEM12 file: {fp}")
        logging.info(f"generating output at {output_fp}")

        start = time.time()

        with open(fp, "r", encoding="UTF-8") as infile, \
            open(output_fp, "w+", encoding="UTF-8") as outfile:

            current_nmi = ""
            current_nmi_interval = ""

            for line in infile:
                if self._is_non_200_300(line):
                    continue

                # NME 200
                if line.startswith(constants.NME12_200):
                    tkns = line.split(constants.NME12_DELIM)
                    current_nmi = tkns[constants.NME12_200_NMI_IDX]
                    current_nmi_interval = tkns[constants.NME12_200_INTERVAL_LEN_IDX]
                    continue

                # NME 300
                tkns = line.split(constants.NME12_DELIM)
                outfile.write(
                    self.augment_nem12_300_with_nmi(line, current_nmi, current_nmi_interval)
                )
                self.num_nmi_date_set.add((current_nmi, tkns[constants.NME12_300_INTERVAL_DATE_IDX]))

        end = time.time()
        logging.info(f"processing completed in {str(datetime.timedelta(seconds=end - start))}s")

        self.generate_statistics(fp)
        return output_fp


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



def run(file: str, log_level: str, output_format: str, hold_ui: True):
    # 1. Add NMI to NEM12 300 data and save the file
    output_file = NEM12Transformer().process(file)

    # 2. Run PySpark
    process_nem12_300(output_file, output_format, log_level, hold_ui)

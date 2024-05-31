import click
import constants
import logging
import multiprocessing
import time
import datetime

from task_manager import TaskManager
from ingestor import Ingestor

@click.command()
@click.option('--file', '-f', help="path to NEM12 file.")
@click.option('--log-level', default="INFO", help="log level")
@click.option('--num-executors', default=multiprocessing.cpu_count(), help="number of executors")
@click.option('--output-format', '-o', type=click.Choice([constants.OUTPUT_FMT_CSV, constants.OUTPUT_FMT_SQL]), default="csv", help="either output to csv or sql with `INSERT` statements")
def main(file: str, log_level: str, num_executors: int, output_format: str):
    logging.basicConfig(
        level=log_level.upper(),
        format='%(asctime)s %(levelname)s:%(name)s:%(message)s'
    )

    tm = TaskManager(constants.DEFAULT_OUTPUT_DIR, output_format, log_level, num_executors)
    ingestor = Ingestor(tm)

    start = time.time()
    tm.start()
    ingestor.ingest(file)
    tm.wait()

    end = time.time()
    logging.info(f"job completed in {str(datetime.timedelta(seconds=end - start))}s")


if __name__ == "__main__":
    main()

import constants
import multiprocessing
import os
import csv
import logging

from dataclasses import dataclass
from typing import List, Dict

@dataclass
class NEM12_200_Block:
    nmi: str
    interval_length: int
    data_records: List[str]

@dataclass
class MeterConsumption:
    nmi: str
    timestamp: str
    consumption: float

    def to_csv_row(self) -> List:
        return [self.nmi, self.timestamp, self.consumption]

    def to_sql_insert(self) -> str:
        return " ".join([
            f"INSERT INTO {constants.DEFAULT_SQL_TABLE_NAME}({constants.COL_NMI}, {constants.COL_TIMESTAMP}, {constants.COL_CONSUMPTION})",
            f"VALUES ('{self.nmi}', {self.timestamp}, {self.consumption})",
            f"ON CONFLICT ({constants.COL_NMI}, {constants.COL_TIMESTAMP}) DO UPDATE SET {constants.COL_CONSUMPTION} = {self.consumption};"
        ])

class Executor:
    def __init__(self, output_format: str, idx: int, queue: multiprocessing.Queue) -> None:
        self.output_format = output_format
        self.idx = idx
        self.queue = queue

        # (nmi, timestamp) -> MeterConsumption
        self.meter_consumption_map = {}


    def sum_interval_values(self, values: List[str]) -> float:
        return sum([float(v) for v in values])

    def make_output_filename(self):
        return f"{constants.DEFAULT_OUTPUT_DIR}/executor-{self.idx}.output.{self.output_format}"

    def output_meter_consumption_to_csv(self):
        with open(self.make_output_filename(), "w+") as outfile:
            writer = csv.writer(outfile)
            for line in self.meter_consumption_map.values():
                writer.writerow(line.to_csv_row())

    def output_meter_consumption_to_sql(self):
        with open(self.make_output_filename(), "w+") as outfile:
            for line in self.meter_consumption_map.values():
                outfile.write(line.to_sql_insert())

    def output_meter_consumption_map(self):
        os.makedirs(constants.DEFAULT_OUTPUT_DIR, exist_ok=True)
        if self.output_format == constants.OUTPUT_FMT_CSV:
            self.output_meter_consumption_to_csv()
        else:
            self.output_meter_consumption_to_sql()

    def run(self):
        logging.info(f"executor {self.idx} running")

        while True:
            item = self.queue.get()
            if item == "EOF":
                self.output_meter_consumption_map()
                logging.info(f"executor {self.idx} stopping")
                break

            for record in item.data_records:
                tkns = record.split(",")
                timestamp = tkns[constants.NME12_300_INTERVAL_DATE_IDX]
                consumption = self.sum_interval_values(tkns[2:49])

                key = (item.nmi, timestamp)
                if key not in self.meter_consumption_map:
                    self.meter_consumption_map[key] = MeterConsumption(item.nmi, timestamp, consumption)
                else:
                    self.meter_consumption_map[key].consumption = consumption


def start_executor(output_format: str, idx: int, log_level: str, queue: multiprocessing.Queue):
    logging.basicConfig(
        level=log_level.upper(),
        format='%(asctime)s %(levelname)s:%(name)s:%(message)s'
    )

    executor = Executor(output_format, idx, queue)
    executor.run()


class TaskManager:
    def __init__(self, output_format: str, log_level: str, num_executors: int) -> None:
        self.num_executors = num_executors
        self.queues = [multiprocessing.Queue() for _ in range(num_executors)]
        self.executors = [
            multiprocessing.Process(
                target=start_executor, args=(output_format, i, log_level, self.queues[i])
            )
            for i in range(num_executors)
        ]

    def nmi_to_executor(self, nmi: str) -> int:
        return hash(nmi) % self.num_executors

    def allocate_200_block_to_executor(self, block: NEM12_200_Block):
        executor_idx = self.nmi_to_executor(block.nmi)
        self.queues[executor_idx].put(block)

    def start(self):
        for executor in self.executors:
            executor.start()

    def stop(self):
        for i in range(self.num_executors):
            self.queues[i].put("EOF")
            self.queues[i].close()

    def wait(self):
        for executor in self.executors:
            executor.join()


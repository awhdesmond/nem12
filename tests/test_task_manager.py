import unittest
import constants
import multiprocessing
import shutil
import time

from os import path
from typing import Dict
from dataclasses import dataclass
from task_manager import TaskManager, MeterConsumption, Executor, NEM12_200_Block

class TestMeterConsumption(unittest.TestCase):
    def test_to_csv_row(self):
        mc = MeterConsumption("nmi", "20201112", 333.456)
        got = mc.to_csv_row()
        want = ["nmi", "20201112", 333.456]

        self.assertEqual(got, want, f"got = {got}, want ={want}")

    def test_to_sql_insert(self):
        mc = MeterConsumption("nmi", "20201112", 333.456)
        got = mc.to_sql_insert()
        want = "INSERT INTO meter_readings(nmi, timestamp, consumption) VALUES('nmi', 20201112, 333.456) ON CONFLICT (nmi, timestamp) DO UPDATE SET consumption = 333.456;\n"

        self.assertEqual(got, want, f"got = {got}, want ={want}")

class TestExecutor(unittest.TestCase):

    def setUp(self):
        self.test_output_dir = "test_output"
        self.queue = multiprocessing.Queue()
        self.executor = Executor(
            self.test_output_dir, constants.OUTPUT_FMT_CSV, 1, self.queue,
        )

    def tearDown(self):
        self.queue.close()
        if path.exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)

    def test_sum_interval_values(self):
        values = ["1.23", "0.456", "0.0", "10"]
        got = self.executor.sum_interval_values(values)
        want = 11.686

        self.assertAlmostEqual(got, want, f"got={got}, want={want}")

    def test_sum_output_filename(self):
        got = self.executor.make_output_filename()
        want = f"{self.test_output_dir}/executor-1.output.csv"
        self.assertEqual(got, want, f"got = {got}, want ={want}")


    def test_output_meter_consumption_map(self):
        @dataclass
        class TestCase:
            output_fmt: str
            consumption_dict: Dict
            want: str

        cases = [
            TestCase(
                constants.OUTPUT_FMT_SQL,
                {
                    ("nmi", "20201112"): MeterConsumption("nmi", "20201112", 333.456),
                    ("nmi", "20201113"): MeterConsumption("nmi", "20201113", 222.456)
                },
                "INSERT INTO meter_readings(nmi, timestamp, consumption) VALUES('nmi', 20201112, 333.456) ON CONFLICT (nmi, timestamp) DO UPDATE SET consumption = 333.456;\nINSERT INTO meter_readings(nmi, timestamp, consumption) VALUES('nmi', 20201113, 222.456) ON CONFLICT (nmi, timestamp) DO UPDATE SET consumption = 222.456;\n"
            ),
            TestCase(
                constants.OUTPUT_FMT_CSV,
                {
                    ("nmi", "20201112"): MeterConsumption("nmi", "20201112", 333.456),
                    ("nmi", "20201113"): MeterConsumption("nmi", "20201113", 222.456)
                },
                "nmi,timestamp,consumption\nnmi,20201112,333.456\nnmi,20201113,222.456\n"
            ),
        ]

        for c in cases:
            self.executor.output_format = c.output_fmt
            self.executor.meter_consumption_map = c.consumption_dict
            self.executor.output_meter_consumption_map()
            with open(self.executor.make_output_filename(), "r") as infile:
                got = infile.read()
                self.assertEqual(got, c.want, f"got = {got}, want ={c.want}")

    def test_run(self):
        self.queue.put(NEM12_200_Block("nmi", ["300,20201112,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,333.456,A,,,20050405003650,\n"]))
        self.queue.put("EOF")
        self.executor.run()

        with open(self.executor.make_output_filename(), "r") as infile:
            got = infile.read()
            want = "nmi,timestamp,consumption\nnmi,20201112,333.456\n"
            self.assertEqual(got, want, f"got = {got}, want ={want}")

class TestTaskManager(unittest.TestCase):

    def setUp(self):
        self.tm = TaskManager(constants.DEFAULT_OUTPUT_DIR, constants.OUTPUT_FMT_CSV, "info", 1)

    def test_start_stop_wait(self):
        self.tm.start()
        time.sleep(2)
        self.tm.stop()
        self.tm.wait()

import unittest
import constants
import time
import shutil

from os import path
from ingestor import Ingestor
from task_manager import TaskManager, NEM12_200_Block

class TestIngestor(unittest.TestCase):

    def setUp(self):
        self.test_output_dir="test_output"
        self.tm = TaskManager(self.test_output_dir, constants.OUTPUT_FMT_CSV, "info", 1)
        self.tm.start()
        self.ingestor = Ingestor(self.tm)

    def tearDown(self):
        if path.exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)

    def test_ingest(self):
        test_file = "examples/nem12-sample.csv"
        self.ingestor.ingest(test_file)
        time.sleep(2)

        with open("test_output/executor-0.output.csv", "r") as f:
            got = f.read()
            want = """NEM1202022,20050401,0.0
NEM1202022,20050402,0.021
NEM1202022,20050403,1866.6820000000002
NEM1202022,20050404,1376.3999999999999
NEM1201004,20050327,321.91
NEM1201004,20050328,288.6499999999999
NEM1201004,20050329,313.76
NEM1201004,20050330,226.23
NEM1202024,20050327,8739.200000000004
NEM1202024,20050328,8857.750000000002
NEM1202024,20050329,8765.410000000003
NEM1202024,20050330,8948.86
NEM1203044,20050327,450.4700000000001
NEM1203044,20050328,448.66999999999973
NEM1203044,20050329,488.20999999999987
NEM1203044,20050330,457.3299999999998
"""
            self.assertEqual(got, want, f"got = {got}, want ={want}")

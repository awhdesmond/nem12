import constants
import os
import logging
import json
import time
import datetime

from tqdm import tqdm
from pathlib import Path
from task_manager import TaskManager, NEM12_200_Block

class Ingestor:

    def __init__(self, tm: TaskManager):
        self.tm = tm

    def process(self, fp: str) ->  str :
        logging.info(f"ingesting NEM12 file: {fp}")

        start = time.time()

        with open(fp, "r", encoding="UTF-8") as infile:
            current_nmi_block = None

            with tqdm() as pbar:
                for line in infile:
                    pbar.update(1)

                    # NME 200
                    if line.startswith(constants.NME12_200):
                        if current_nmi_block is not None:
                            self.tm.allocate_200_block_to_executor(current_nmi_block)

                        # tkns = line.split(constants.NME12_DELIM)
                        # nmi = tkns[constants.NME12_200_NMI_IDX]
                        # nmi_interval = tkns[constants.NME12_200_INTERVAL_LEN_IDX]

                        second_comma_idx = line.index(",", 4)
                        nmi = line[3:second_comma_idx]
                        current_nmi_block = NEM12_200_Block(nmi, 30, [])
                        continue

                    # NME 300
                    if line.startswith(constants.NME12_300):
                        current_nmi_block.data_records.append(line)

        end = time.time()
        logging.info(f"ingesting completed in {str(datetime.timedelta(seconds=end - start))}s")
        self.tm.stop()

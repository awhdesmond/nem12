import constants
import logging
import time
import datetime

from tqdm import tqdm
from task_manager import TaskManager, NEM12_200_Block


class Ingestor:

    def __init__(self, tm: TaskManager):
        self.tm = tm

    def ingest(self, fp: str) ->  str :
        logging.info(f"ingesting NEM12 file: {fp}")

        pbar = tqdm()
        start = time.time()

        with open(fp, "r", encoding="UTF-8") as infile:
            current_nmi_block = None

            for line in infile:
                pbar.update(1)
                # NME 200
                if line.startswith(constants.NME12_200):
                    if current_nmi_block is not None:
                        self.tm.allocate_200_block_to_executor(current_nmi_block)

                    tkns = line.split(",")
                    current_nmi_block = NEM12_200_Block(
                        nmi=tkns[constants.NME12_200_NMI_IDX],
                        data_records=[],
                        interval_length=int(tkns[constants.NME12_200_INTERVAL_LEN_IDX])
                    )
                    continue

                # NME 300
                if line.startswith(constants.NME12_300):
                    current_nmi_block.data_records.append(line)

        end = time.time()
        logging.info(f"ingesting completed in {str(datetime.timedelta(seconds=end - start))}s")
        pbar.close()
        self.tm.stop()

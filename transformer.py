import constants
import json
import os
import logging
import time
import datetime

from pathlib import Path

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

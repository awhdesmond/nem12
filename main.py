import click
import transformer
import constants
import logging

from spark import process_nem12_300

@click.command()
@click.option('--file', '-f', help="path to NEM12 file.")
@click.option('--log-level', default="INFO", help="log level")
@click.option('--output-format', '-o', type=click.Choice([constants.OUTPUT_FMT_CSV, constants.OUTPUT_FMT_SQL]), default="csv", help="either output to csv or sql with `INSERT` statements")
@click.option('--hold-ui', is_flag=True, default=False, help="will keep spark session running until enter is pressed")
def main(file: str, log_level: str, output_format: str, hold_ui: True):

    logging.basicConfig(level=log_level)

    # 1. Add NMI to NEM12 300 data and save the file
    nem12 = transformer.NEM12Transformer()
    output_file = nem12.process(file)

    # 2. Run PySpark
    process_nem12_300(output_file, output_format, log_level, hold_ui)


if __name__ == "__main__":
    main()

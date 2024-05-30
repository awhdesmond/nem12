import click
import transformer
import constants

from spark import process_nem300

@click.command()
@click.option('--file', '-f', help="path to NEM12 file.")
@click.option('--log-level', default="INFO", help="log level")
@click.option('--coalesce', is_flag=True, default=False, help="whether to coalesce spark output")
@click.option('--output-format', '-o', type=click.Choice([constants.OUTPUT_FMT_CSV, constants.OUTPUT_FMT_SQL]), default="csv", help="either output to csv or sql with `INSERT` statements")
def main(file: str, log_level: str, coalesce: bool, output_format: str):

    # 1. Add NMI to NEM12 300 data and save the file
    nem12 = transformer.NEM12Transformer()
    output_file = nem12.process(file)

    # 2. Run PySpark
    process_nem300(output_file, output_format, coalesce, log_level)


if __name__ == "__main__":
    main()

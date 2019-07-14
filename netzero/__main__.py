"""Entry point for NetZero data collector

Provides the command line interface for collecting data from the various soures.
"""

import sqlite3
import datetime
import configparser

import netzero.sources


def collect_data(arguments):
    if arguments.config:
        config = configparser.read_file(arguments.config)
    else:
        config = None

    conn = sqlite3.connect(arguments.database,
                           detect_types=sqlite3.PARSE_DECLTYPES)

    initialized_sources = []
    for source in arguments.sources:
        initialized_sources.append(source(config, conn))

    for source in initialized_sources:
        # Collect the raw data from the source
        source.collect_data(start_date=arguments.start, end_date=arguments.end)

        # Process the data from the source
        source.process_data()


def format_data(arguments):
    print("format", arguments)


def main():
    import argparse

    program_name = "netzero"

    parser = argparse.ArgumentParser(
        program_name,
        description="Collects and formats data from multiple sources")
    parser.set_defaults(func=None)

    subparsers = parser.add_subparsers(title="available commands",
                                       metavar="<command>",
                                       prog=program_name)

    # --- Data Collection Arguments ---
    collect_parser = subparsers.add_parser(
        "collect",
        description=
        "Collect data from various sources and store it in a local database",
        help="Collect data",
        prefix_chars="-+")
    collect_parser.set_defaults(func=collect_data)

    sources_group = collect_parser.add_argument_group(
        "data sources",
        description="Flags used to select the supported data sources")
    sources_group.add_argument("+s",
                               "--solar",
                               help="collects solar data",
                               dest="sources",
                               action="append_const",
                               const=netzero.sources.Solar)
    sources_group.add_argument("+p",
                               "--pepco",
                               help="collects pepco data",
                               dest="sources",
                               action="append_const",
                               const=netzero.sources.Pepco)
    sources_group.add_argument("+g",
                               "--gshp",
                               help="collects ground source heat pump data",
                               dest="sources",
                               action="append_const",
                               const=netzero.sources.Gshp)
    sources_group.add_argument("+w",
                               "--weather",
                               help="collects weather data",
                               dest="sources",
                               action="append_const",
                               const=netzero.sources.Weather)

    collect_parser.add_argument("-s",
                                "--start",
                                metavar="YYYY-MM-DD",
                                help="start date for date range",
                                dest="start",
                                type=datetime.date.fromisoformat)
    collect_parser.add_argument("-e",
                                "--end",
                                metavar="YYYY-MM-DD",
                                help="end date for date range",
                                dest="end",
                                type=datetime.date.fromisoformat)

    collect_parser.add_argument(
        "-c",
        required=True,
        metavar="config",
        help="loads inputs from the specified JSON file",
        dest="config",
        type=argparse.FileType('r'))

    collect_parser.add_argument("database")

    # --- Formatting Arguments ---
    format_parser = subparsers.add_parser("format",
                                          description="Format data",
                                          help="Format data")
    format_parser.set_defaults(func=format_data)

    # --- Logic ---
    arguments = parser.parse_args()

    if arguments.func is not None:
        arguments.func(arguments)
    else:
        parser.print_help()


# TODO -- ADD Defaults Option (--defaults allows you to use all the default options)
if __name__ == "__main__":
    main()
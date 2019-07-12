"""Entry point for NetZero data collector

Provides the command line interface for collecting data from the various soures.
"""

import netzero.sources
import sqlite3, datetime, json


def collect_data(arguments):
    if arguments.config:
        config = json.load(arguments.config)
    else:
        config = None

    conn = arguments.database

    initialized_sources = []
    for source in arguments.sources:
        initialized_sources.append(source(config, conn))

    for source in initialized_sources:
        # Collect the raw data from the source
        source.collect_data(start_date=arguments.begin, end_date=arguments.end)

        # Process the data from the source
        source.process_data()


def format_data(arguments):
    print("format", arguments)


# TODO -- ADD Defaults Option (--defaults allows you to use all the default options)
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Automatically collects and formats timeseries data from multiple sources")
    parser.set_defaults(func=None)

    subparsers = parser.add_subparsers(dest="action")
    
    # --- Data Collection Arguments ---
    collect_parser = subparsers.add_parser("collect", description="Collect data")
    collect_parser.set_defaults(func=collect_data)

    sources_group = collect_parser.add_argument_group("Data Sources", description="Flags used to select the supported data sources")
    sources_group.add_argument("-s", "--solar", help="Collects solar data", dest="sources", action="append_const", const=netzero.sources.Solar)
    sources_group.add_argument("-p", "--pepco", help="Collects pepco data", dest="sources", action="append_const", const=netzero.sources.Pepco)
    # sources_group.add_argument("-g", "--gshp", help="Collects ground source heat pump data", dest="sources", action="append_const", const=netzero.sources.Gshp)
    sources_group.add_argument("-w", "--weather", help="Collects weather data", dest="sources", action="append_const", const=netzero.sources.Weather)

    collect_parser.add_argument("-c", required=True, metavar="config", help="Loads inputs from the specified JSON file", dest="config", type=argparse.FileType('r'))

    def parse_date(string):
        try:
            return datetime.datetime.strptime(string, r"%Y-%m-%d")
        except ValueError:
            raise argparse.ArgumentTypeError("%s doesn't follow the format YYYY-MM-DD" % string)

    collect_parser.add_argument("-b", "--begin", metavar="YYYY-MM-DD", help="Start date for date range", dest="begin", type=parse_date)
    collect_parser.add_argument("-e", "--end", metavar="YYYY-MM-DD", help="End date for date range", dest="end", type=parse_date)

    collect_parser.add_argument("database", type=sqlite3.connect)

    # --- Formatting Arguments ---
    format_parser = subparsers.add_parser("format", description="Format data")
    format_parser.set_defaults(func=format_data)

    # --- Logic ---
    arguments = parser.parse_args()

    if arguments.func is not None:
        arguments.func(arguments)
    else:
        parser.print_help()
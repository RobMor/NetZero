import sqlite3
import datetime
import configparser

import netzero.sources

def add_args(parser):
    sources_group = parser.add_argument_group(
        "data sources",
        description="Flags used to select the supported data sources")

    for name, source in netzero.sources.load():
        if not hasattr(source, "name"):
            source.name = name
        # TODO potential short hand collisions
        if not hasattr(source, "option"):
            source.option = source.name[0]
        if not hasattr(source, "long_option"):
            source.long_option = source.name

        sources_group.add_argument(
            "+" + source.option,  # Shorthand argument
            "--" + source.long_option,  # Longform argument
            help=source.summary,
            dest="sources",
            action="append_const",
            const=source)

    parser.add_argument("-s",
                                "--start",
                                metavar="YYYY-MM-DD",
                                help="start date for date range",
                                dest="start",
                                type=datetime.date.fromisoformat)
    parser.add_argument("-e",
                                "--end",
                                metavar="YYYY-MM-DD",
                                help="end date for date range",
                                dest="end",
                                type=datetime.date.fromisoformat)

    parser.add_argument(
        "-c",
        required=True,
        metavar="config",
        help="loads inputs from the specified INI file",
        dest="config",
        type=argparse.FileType('r'))

    parser.add_argument("database")

def collect_data(arguments):
    if arguments.config:
        config = configparser.read_file(arguments.config)
    else:
        config = None

    conn = sqlite3.connect(arguments.database,
                           detect_types=sqlite3.PARSE_DECLTYPES)

    # Load configurations into sources before collecting data
    # This lets the user respond to config errors early
    initialized_sources = []
    for source in arguments.sources:
        initialized_sources.append(source(config))

    for source in initialized_sources:
        print("Collecting data for", source.name)

        # Collect the raw data from the source
        source.collect_data(start_date=arguments.start, end_date=arguments.end)

        # Process the data from the source
        source.process_data()

def main():
    print("COLLECT")
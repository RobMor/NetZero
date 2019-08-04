import sqlite3
import datetime
import argparse
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

    parser.add_argument("-c",
                        required=True,
                        metavar="config",
                        help="loads inputs from the specified INI file",
                        dest="config",
                        type=argparse.FileType('r'))

    parser.add_argument("database")


def main(arguments):
    if arguments.config:
        config = configparser.ConfigParser()
        config.read_file(arguments.config)
    else:
        config = None

    conn = sqlite3.connect(arguments.database,
                           detect_types=sqlite3.PARSE_DECLTYPES)

    sources = arguments.sources

    # Load configurations into sources before collecting data
    # This lets the user respond to config errors early
    sources = [source(config) for source in sources]

    for source in sources:
        # Create a string for sqlite subtitution
        substitutions = "(" + ",".join(["?"] * len(source.columns)) + ")"

        # Create the table
        query = f"CREATE TABLE IF NOT EXISTS ?{substitutions}"
        conn.execute(query, (source.name, *source.columns))

        # Insert the data
        query = f"INSERT OR IGNORE INTO ?{substitutions} VALUES {substitutions}"
        for row in source.collect_data(arguments.start, arguments.end):
            conn.execute(query, (source.name, *source.columns, *row))

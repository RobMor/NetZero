import sqlite3
import datetime
import argparse
import configparser

import netzero.sources


def add_args(parser):
    netzero.sources.add_source_args(parser)

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
        # Create the table
        query = f"CREATE TABLE IF NOT EXISTS {source.name}{source.columns}"
        conn.execute(query)

        # Create a list of columns without type annotations
        columns = tuple(map(lambda x: x.split()[0], source.columns))

        # Create a string for sqlite subtitution
        substitutions = "(" + ",".join(["?"] * len(source.columns)) + ")"

        # Create the query
        query = f"INSERT OR IGNORE INTO {source.name}{columns} VALUES {substitutions}"

        # Collect the data
        for row in source.collect_data(arguments.start, arguments.end):
            conn.execute(query, row)

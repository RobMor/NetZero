import sqlite3
import datetime
import argparse
import configparser

from sqlalchemy.orm import sessionmaker

import netzero.sources


def add_args(parser):
    netzero.sources.add_args(parser)
    netzero.db.add_args(parser)

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


def main(arguments):
    if arguments.config:
        config = configparser.ConfigParser()
        config.read_file(arguments.config)
    else:
        config = None

    sources = arguments.sources

    # Load configurations into sources before collecting data
    # This lets the user respond to config errors early
    sources = [source(config) for source in sources]

    engine = netzero.db.main(arguments)
    Session = sessionmaker(bind=engine)

    for source in sources:
        session = Session()
        source.collect_data(session, arguments.start, arguments.end)
        session.close()

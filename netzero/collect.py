import argparse
import datetime

import netzero.sources
import netzero.db
import netzero.config


def add_args(parser):
    netzero.sources.add_args(parser)
    netzero.db.add_args(parser)
    netzero.config.add_args(parser)

    parser.add_argument(
        "-s",
        "--start",
        metavar="YYYY-MM-DD",
        help="start date for date range",
        dest="start",
        type=datetime.date.fromisoformat,
    )
    parser.add_argument(
        "-e",
        "--end",
        metavar="YYYY-MM-DD",
        help="end date for date range",
        dest="end",
        type=datetime.date.fromisoformat,
    )


def main(arguments):
    config = netzero.config.load_config(arguments.config)

    sources = arguments.sources

    # Load configurations into sources before collecting data
    # This lets the user respond to config errors early
    sources = [source(config, arguments.database) for source in sources]

    for source in sources:
        source.collect(arguments.start, arguments.end)

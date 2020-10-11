import argparse
import csv
import datetime

import netzero.sources
import netzero.db
import netzero.util


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

    parser.add_argument("output", help="the file to export data to")


def main(arguments):
    if not hasattr(arguments, "sources") or arguments.sources is None:
        print("No sources specified, nothing to export")
        return

    config = netzero.config.load_config(arguments.config)

    # Load configurations into sources early so user can respond to errors
    sources = [source(config, arguments.database) for source in arguments.sources]

    start_date = arguments.start
    end_date = arguments.end

    # TODO what if all min/max dates return none?

    if start_date is None:
        start_date = min([source.min_date() for source in sources])

    if end_date is None:
        end_date = max([source.max_date() for source in sources])

    cursors = []

    for source in sources:
        cursors.append(source.format(start_date, end_date))

    with open(arguments.output, "w") as f:
        writer = csv.writer(f)

        header = ["date"]
        for source in sources:
            header.append(source.name)

        writer.writerow(header)

        for date in netzero.util.iter_days(start_date, end_date):
            netzero.util.print_status(
                "Format", "Exporting: {}".format(date.strftime("%Y-%m-%d"))
            )

            row = [date.strftime("%Y-%m-%d")]
            for cursor in cursors:
                row.append(cursor.fetchone()[0])

            writer.writerow(row)

    netzero.util.print_status("Format", "Exporting Complete", newline=True)

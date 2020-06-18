import argparse
import configparser
import datetime
import sqlite3
import threading

from tqdm import tqdm

import netzero.sources


def add_args(parser):
    netzero.sources.add_args(parser)

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

    parser.add_argument(
        "-c",
        required=True,
        metavar="config",
        help="loads inputs from the specified INI file",
        dest="config",
        type=argparse.FileType("r"),
    )


def main(arguments):
    if arguments.config:
        config = configparser.ConfigParser()
        config.read_file(arguments.config)
    else:
        config = None

    db_location = config["general"]["database_location"]

    sources = arguments.sources

    # Configure source objects before collecting data.
    # This lets the user respond to config errors early.
    configured_sources = []
    for source in sources:
        # TODO clean error handling here!
        # Each source gets their own connection so we can bypass the check for 
        # same thread...
        conn = sqlite3.connect(db_location, check_same_thread=False)
        configured_source = source(config, conn)
        configured_sources.append(configured_source)

    collection_threads = []
    progress_bars = []
    for i, source in enumerate(configured_sources):
        progress_bar = tqdm(
            unit="query",
            dynamic_ncols=True,
            position=i
        )
        source.progress_bar = progress_bar

        thread = threading.Thread(target=source.collect, args=(arguments.start, arguments.end))
        thread.start()

        collection_threads.append(thread)
        progress_bars.append(progress_bar)

    # TODO keyboard interrupts don't work properly
    for thread in collection_threads:
        thread.join()


# class CollectionThread(threading.Thread):
#     def __init__(self, source, start, end, bar_offset=None):
#         super().__init__()

#         self.source = source
#         self.start_date = start
#         self.end_date = end
#         self.bar_offset = bar_offset

#     def run(self):
#         # Set up a progress bar that will be accessed via SourceBase
#         self.source.progress_bar = tqdm(
#             unit="query",
#             dynamic_ncols=True,
#             position=self.bar_offset
#         )

#         self.source.collect(self.start_date, self.end_date)

#         self.source.progress_bar.close()
#         self.source.progress_bar.refresh()

"""Entry point for NetZero data collector

Provides the command line interface for collecting data from the various soures.
"""

import datetime
import configparser

import entrypoints

import netzero.sources
import netzero.collect
import netzero.format


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
        "Collect data from various sources",
        help="Collect data",
        prefix_chars="-+")
    collect_parser.set_defaults(func=netzero.collect.main)

    netzero.collect.add_args(collect_parser)

    # --- Formatting Arguments ---
    format_parser = subparsers.add_parser("format",
                                          description="Format data",
                                          help="Format data",
                                          prefix_chars="-+")
    format_parser.set_defaults(func=netzero.format.main)

    netzero.format.add_args(format_parser)

    # --- Logic ---
    arguments = parser.parse_args()

    if arguments.func is not None:
        arguments.func(arguments)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
#!/usr/bin/env python3

import csv
import argparse
import datetime
import configparser

import sqlalchemy

import netzero.sources
import netzero.util


def add_args(parser):
    netzero.sources.add_args(parser)

    parser.add_argument("-c",
                        required=True,
                        metavar="config",
                        help="loads inputs from the specified INI file",
                        dest="config",
                        type=argparse.FileType('r'))

    parser.add_argument("output")


def export(conn, filename):
    raise NotImplementedError()
    # Using pandas for the OUTER JOIN functionality
    pepco = pd.read_sql_query("SELECT day, value AS pepco FROM pepco_day",
                              conn)
    solar = pd.read_sql_query("SELECT day, value AS solar FROM solar_day",
                              conn)
    gshp = pd.read_sql_query("SELECT day, value AS gshp FROM gshp_day", conn)
    weather = pd.read_sql_query(
        "SELECT day, value AS weather FROM weather_day", conn)

    out = pepco.merge(solar, how="outer",
                      on="day").merge(gshp, how="outer",
                                      on="day").merge(weather,
                                                      how="outer",
                                                      on="day")

    out.to_csv(filename, index=False)

def main(arguments):
    if arguments.config:
        config = configparser.ConfigParser()
        config.read_file(arguments.config)
    else:
        config = None

    # Load configurations into sources before collecting data
    # This lets the user respond to config errors early
    sources = [source(config) for source in arguments.sources]

    data = []

    min_date = datetime.date.max
    max_date = datetime.date.min

    for source in sources:
        source_min_date = source.min_date()
        source_max_date = source.max_date()

        if not type(source_min_date) is datetime.date or not type(source_max_date) is datetime.date:
            raise TypeError("Format was given datetime for min/max date, expected date")

        min_date = min(min_date, source_min_date)
        max_date = max(max_date, source_max_date)

        data.append(source.format())

    with open(arguments.output, 'w') as f:
        writer = csv.writer(f)

        header = ["date"]
        for source in sources:
            header.append(source.name)

        writer.writerow(header)

        for date in netzero.util.iter_days(min_date, max_date):
            netzero.util.print_status("Format", "Exporting: {}".format(date.strftime("%Y-%m-%d")))

            row = [date.strftime("%Y-%m-%d")]
            for d in data:
                row.append(d.get(date))
            
            writer.writerow(row)

    netzero.util.print_status("Format", "Exporting Complete", newline=True)
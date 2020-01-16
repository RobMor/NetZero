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
    netzero.db.add_args(parser)

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

    sources = arguments.sources

    # Load configurations into sources before collecting data
    # This lets the user respond to config errors early
    sources = [source(config) for source in sources]

    engine = netzero.db.main(arguments)
    Session = sqlalchemy.orm.sessionmaker(bind=engine)

    data = []

    min_date = datetime.datetime.max
    max_date = datetime.datetime.min
    for source in sources:
        session = Session()

        source_min = source.min_date(session)
        source_max = source.max_date(session)

        min_date = min(min_date, source_min)
        max_date = max(max_date, source_max)

        print("{}: {} to {}".format(source.name, source_min.isoformat(), source_max.isoformat()))

        data.append(source.format(session))


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
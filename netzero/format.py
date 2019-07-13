#!/usr/bin/env python3

import sqlite3, csv
import pandas as pd
from sys import argv


def export(conn, filename):
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


usage = """
Usage:
export.py [database] [output filename]"""

if __name__ == "__main__":
    if len(argv) < 3:
        print(usage)
    else:
        conn = sqlite3.connect(argv[1])
        export(conn, argv[2])
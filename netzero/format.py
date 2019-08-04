#!/usr/bin/env python3

import sqlite3, csv
import pandas as pd
from sys import argv

def add_args(parser):
    pass

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

def main():
    print("FORMAT")

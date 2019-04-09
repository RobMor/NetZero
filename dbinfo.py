#!/usr/bin/env python3

import sqlite3
from sys import argv

usage = """
Usage:
dbinfo.py [database]"""

if __name__ == "__main__":
    if len(argv) < 2:
        print(usage)
    else:
        conn = sqlite3.connect(argv[1])
        cur = conn.cursor()

        for table in ["pepco_day", "solar_day", "gshp_day", "weather_day"]:
            cur.execute("SELECT MIN(day) AS start, MAX(day) AS end FROM "+table)
            result = cur.fetchone()
            print(table,":",result[0], "-", result[1])
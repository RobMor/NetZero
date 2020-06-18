"""Data source specification for Solar Edge data.

SolarEdge supplies an API with their solar panels which allows you to view
the power supplied by you panels in high detail. In this module we collect that
data and convert it into a useful daily summation of energy production.

Solar Edge API documentation (ca 2019):
https://www.solaredge.com/sites/default/files/se_monitoring_api.pdf
"""
import datetime
import json
import os
import requests
import sqlite3
import math

from netzero.sources import SourceBase
import netzero.util


class Solar(SourceBase):
    name = "solaredge"
    summary = "Solar Edge data"

    default_start = datetime.date(2016, 1, 27)
    default_end = datetime.date.today()

    def __init__(self, config, conn):
        config = netzero.util.validate_config(
            config, entry="SolarEdge", fields=["api_key", "site_id"]
        )

        self.api_key = config["api_key"]
        self.site_id = config["site_id"]

        self.conn = conn

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS solaredge (time TIMESTAMP PRIMARY KEY, watt_hrs FLOAT)"
        )

    def collect(self, start_date=None, end_date=None):
        """Collect raw solar data from SolarEdge

        Collects data using the SolarEdge API, storing it in the database.
        
        Parameters
        ----------
        start : datetime.date, optional
            The end of the time interval to collect data from
        end : datetime.date, optional
            The end of the time interval to collect data from
        """
        if start_date is None:
            start_date = self.default_start
        if end_date is None:
            end_date = self.default_end

        cur = self.conn.cursor()

        total = math.ceil((end_date - start_date).days / 30)

        self.reset_status("SolarEdge", "Collecting Data", total)

        # Iterate through each date range
        for i, interval in enumerate(netzero.util.time_intervals(start_date, end_date, days=30)):
            self.set_progress(
                "{} to {}".format(
                    interval[0].strftime("%Y-%m-%d"), interval[1].strftime("%Y-%m-%d")
                ),
                i,
            )

            result = self.query_api(interval[0], interval[1])

            for entry in result["energy"]["values"]:
                date = datetime.datetime.strptime(entry["date"], "%Y-%m-%d %H:%M:%S")
                value = entry["value"] or 0

                cur.execute(
                    "INSERT OR IGNORE INTO solaredge VALUES (?, ?)", (date, value)
                )

            self.conn.commit()

        cur.close()

        self.reset_status("Weather (NCDC API)", "Complete", 0)
        self.finish_progress()

    def query_api(self, start_date, end_date):
        """A method to query the Solar Edge api for energy data

        Parameters
        ----------
        start_date : datetime.date
            The start of the time interval to query the api for
        end_date : datetime.date
            The end of the time interval to query the api for

        Returns
        -------
        The API response in python dict format:
            {
                "energy":{
                    "timeUnit": _,
                    "unit":_,
                    "values":[
                        {
                            "date":"YYYY-MM-DD HH:MM:SS",
                            "value":_
                        }, ...
                    ]
                }
            }
        """
        payload = {
            "api_key": self.api_key,
            "startDate": start_date.strftime("%Y-%m-%d"),
            "endDate": end_date.strftime("%Y-%m-%d"),
            # Even though we condense this data down to a daily sum we still want to
            # collect as much data as possible because perhaps it may some day be
            # useful. Because of this we do quarter of an hour
            "timeUnit": "QUARTER_OF_AN_HOUR",
        }
        data = requests.get(
            "https://monitoringapi.solaredge.com/site/" + self.site_id + "/energy.json",
            params=payload,
        )
        return json.loads(data.text)

    def min_date(self):
        result = self.conn.execute("SELECT date(min(time)) FROM solaredge").fetchone()[
            0
        ]

        return datetime.datetime.strptime(result, "%Y-%m-%d").date()

    def max_date(self):
        result = self.conn.execute("SELECT date(max(time)) FROM solaredge").fetchone()[
            0
        ]

        return datetime.datetime.strptime(result, "%Y-%m-%d").date()

    def format(self):
        netzero.util.print_status("SolarEdge", "Querying Database")

        data = self.conn.execute(
            "SELECT date(time), SUM(watt_hrs) / 1000 FROM solaredge GROUP BY date(time)"
        ).fetchall()

        result = {
            datetime.datetime.strptime(date, "%Y-%m-%d").date(): value
            for date, value in data
        }

        netzero.util.print_status("SolarEdge", "Complete", newline=True)

        return result

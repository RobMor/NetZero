import datetime
import json
import os
import requests
import sqlite3
import math

from netzero.sources import SourceBase
import netzero.util


class Weather(SourceBase):
    name = "weather"
    summary = "weather data"

    default_start = datetime.date(2014, 1, 1)
    default_end = datetime.date.today()

    def __init__(self, config, conn):
        config = netzero.util.validate_config(
            config, entry="NCDC", fields=["api_key", "stations"]
        )

        self.api_key = config["api_key"]
        self.stations = json.loads(config["stations"])

        self.conn = conn

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS weather (date DATE, temperature FLOAT, station TEXT, PRIMARY KEY (date, station))"
        )

    def collect(self, start_date=None, end_date=None):
        """Collect the raw weather data from NCDC API

        Parameters
        ----------
        start_date: datetime.date, optional
            The start of the time interval to collect data for
        end_date: datetime.date, optional
            The end of the time interval to collect data for
        """
        if start_date is None:
            start_date = self.default_start
        if end_date is None:
            end_date = self.default_end

        cur = self.conn.cursor()

        # Maximum return is 1000 entries
        num_days = 1000 // len(self.stations)
        # Maximum date-range is 1 year
        if num_days > 365:
            num_days = 365

        total = math.ceil((end_date - start_date).days / num_days)

        self.reset_status("Weather (NCDC API)", "Collecting Data", total)

        for i, interval in enumerate(netzero.util.time_intervals(
            start_date, end_date, days=num_days
        )):
            self.set_progress(
                "{} to {}".format(
                    interval[0].strftime("%Y-%m-%d"), interval[1].strftime("%Y-%m-%d")
                ),
                i,
            )

            # TODO -- REMOVE ASSUMPTION THAT LEN(DATA) < LIMIT
            raw_data = self.query_api(interval[0], interval[1])

            if raw_data is None:
                print("ERROR QUERYING API")  # TODO exception here?
                continue

            for entry in raw_data.get("results", []):
                # Insert the weather data to the table, to be averaged later
                date = datetime.datetime.strptime(
                    entry["date"], "%Y-%m-%dT%H:%M:%S"
                ).date()
                value = entry["value"]
                station = entry["station"]

                cur.execute(
                    "INSERT OR IGNORE INTO weather VALUES (?, ?, ?)", (date, value, station)
                )

            self.conn.commit()

        cur.close()

        self.reset_status("Weather (NCDC API)", "Complete", 0)
        self.finish_progress()

    def query_api(self, start_date, end_date):
        """Query the NCDC API for average daily temperature data

        Parameters
        ----------
        start_date : datetime.date
            Start of time range to collect data for
        end_date : datetime.date
            End of time range to collect data for

        Returns
        -------
        A python dict containing the data in the format:
            {
                "results":[
                    {
                        "date":"YYYY-MM-DDTHH:MM:SS",
                        "value":_
                    }, ...
                ]
            }
        """
        headers = {"token": self.api_key}
        params = {
            "datasetid": "GHCND",  # Daily weather
            "stationid": self.stations,
            "datatypeid": "TMAX",  # Max Temperature
            "units": "standard",  # Fahrenheit
            "limit": 1000,  # Maximum request size
            "startdate": start_date.strftime("%Y-%m-%d"),
            "enddate": end_date.strftime("%Y-%m-%d"),
        }

        response = requests.get(
            "https://www.ncdc.noaa.gov/cdo-web/api/v2/data",
            headers=headers,
            params=params,
        )

        if response.ok:
            return response.json()
        else:
            print(response.text)
            return None

    def min_date(self):
        result = self.conn.execute("SELECT min(date) FROM weather").fetchone()[0]

        return datetime.datetime.strptime(result, "%Y-%m-%d").date()

    def max_date(self):
        result = self.conn.execute("SELECT max(date) FROM weather").fetchone()[0]

        return datetime.datetime.strptime(result, "%Y-%m-%d").date()

    def format(self):
        netzero.util.print_status("Weather", "Querying Database")

        data = self.conn.execute(
            "SELECT date, AVG(temperature) FROM weather GROUP BY date"
        ).fetchall()

        result = {
            datetime.datetime.strptime(date, "%Y-%m-%d").date(): value
            for date, value in data
        }

        netzero.util.print_status("Weather", "Complete", newline=True)

        return result

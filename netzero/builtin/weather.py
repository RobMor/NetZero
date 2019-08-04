import sqlite3
import datetime
import requests
import json

from netzero.sources import DataSource
from netzero import util


class Weather(DataSource):
    columns = (DataSource.TIME, "value", "station"
               )  # TODO allow for primary keys
    summary = "collects weather data"

    default_start = datetime.datetime(2014, 1, 1)
    default_end = datetime.datetime.today()

    def __init__(self, config):
        util.validate_config(config,
                             entry="weather",
                             fields=["api_key", "stations"])

        self.api_key = config["weather"]["api_key"]
        self.stations = json.loads(config["weather"]["stations"])

    def collect_data(self, start_date=None, end_date=None):
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

        # Maximum return is 1000 entries
        num_days = 1000 // len(self.stations)
        # Maximum date-range is 1 year
        if num_days > 365:
            num_days = 365

        for interval in util.time_intervals(start_date,
                                            end_date,
                                            days=num_days):
            # TODO -- REMOVE ASSUMPTION THAT LEN(DATA) < LIMIT
            raw_data = self.query_api(interval[0], interval[1])

            if raw_data is None:
                print("Error querying NCDC API")
                continue

            for entry in raw_data.get("results", []):
                # Insert the weather data to the table, to be averaged later
                date = datetime.datetime.fromisoformat(entry["date"])
                value = entry["value"]
                station = entry["station"]

                yield date, value, station

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
            "datatypeid": "TAVG",  # Average Temperature
            "units": "standard",  # Fahrenheit
            "limit": 1000,  # Maximum request size
            "startdate": start_date.strftime("%Y-%m-%d"),
            "enddate": end_date.strftime("%Y-%m-%d")
        }

        response = requests.get(
            "https://www.ncdc.noaa.gov/cdo-web/api/v2/data",
            headers=headers,
            params=params)
        if response.ok:
            return response.json()
        else:
            print(response.text)
            return None

    def aggregators(self):
        return {("value", ): WeatherAgg}


class WeatherAgg(object):
    def __init__(self):
        self.sum = 0
        self.count = 0

    def step(self, value):
        self.sum += value
        self.count += 1

    def finalize(self):
        return self.sum / self.count
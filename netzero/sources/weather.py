import sqlite3
import datetime
import requests

from netzero.sources.base import DataSource
from netzero.sources import util


class Weather(DataSource):
    default_start = datetime.datetime(2014, 1, 1)
    default_end = datetime.datetime.today()

    def __init__(self, config, conn):
        super().validate_config(config, entry="weather", fields=["api_key", "stations"])

        self.api_key = config["weather"]["api_key"]
        self.stations = config["weather"]["stations"]

        self.conn = conn

        with self.conn:
            # Create the table for the raw data
            # We collect data from multiple stations for each day so date cant be primary key
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS weather_raw(time TIMESTAMP, value REAL)
            """)
            
            # Create the table for the processed data
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS weather_day(date DATE PRIMARY KEY, value REAL)
            """)

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

        for interval in util.time_intervals(start_date, end_date, days=num_days):
            # TODO -- REMOVE ASSUMPTION THAT LEN(DATA) < LIMIT
            raw_data = self.query_api(interval[0], interval[1])

            if raw_data is None:
                print("Error querying NCDC API")
                continue
            
            with self.conn:
                for entry in raw_data.get("results", []):
                    # Insert the weather data to the table, to be averaged later
                    day = datetime.datetime.fromisoformat(entry["date"])
                    val = entry["value"]

                    self.conn.execute("""
                        INSERT INTO weather_raw(time, value) VALUES(?,?)
                    """, (day, val))

                    print("WEATHER:", day, "--", val)

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
        headers = {
            "token": self.api_key
        }
        params = {
            "datasetid": "GHCND",  # Daily weather
            "stationid": self.stations, 
            "datatypeid": "TAVG",  # Average Temperature
            "units": "standard",  # Fahrenheit
            "limit": 1000,  # Maximum request size
            "startdate": start_date.strftime("%Y-%m-%d"),
            "enddate": end_date.strftime("%Y-%m-%d")
        }   

        response = requests.get("https://www.ncdc.noaa.gov/cdo-web/api/v2/data", headers=headers, params=params)
        if response.ok:
            return response.json()
        else:
            print(response.text)
            return None

    def process_data(self):
        self.conn.execute("""
            INSERT OR IGNORE INTO weather_day
            SELECT
                DATE(time) as realday,
                AVG(value)
            FROM weather_raw GROUP BY realday
        """)

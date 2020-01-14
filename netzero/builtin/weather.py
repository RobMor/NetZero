import datetime
import requests
import json

from sqlalchemy import Column, Date, Float, String

import netzero.db
import netzero.util


class Weather:
    name = "weather"
    summary = "weather data"

    default_start = datetime.date(2014, 1, 1)
    default_end = datetime.date.today()


    def __init__(self, config):
        netzero.util.validate_config(config,
                             entry="weather",
                             fields=["api_key", "stations"])

        self.api_key = config["weather"]["api_key"]
        self.stations = json.loads(config["weather"]["stations"])


    def collect(self, session, start_date=None, end_date=None):
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

        for interval in netzero.util.time_intervals(start_date,
                                            end_date,
                                            days=num_days):
            netzero.util.print_status("Weather", "Collecting: {} to {}".format(interval[0].isoformat(), interval[1].isoformat()))

            # TODO -- REMOVE ASSUMPTION THAT LEN(DATA) < LIMIT
            raw_data = self.query_api(interval[0], interval[1])

            if raw_data is None:
                print("ERROR QUERYING API")  # TODO exception here?
                continue

            session.query(WeatherEntry).filter(
                WeatherEntry.date.between(interval[0], interval[1]) # TODO test boundaries
            ).delete(synchronize_session=False)

            for entry in raw_data.get("results", []):
                # Insert the weather data to the table, to be averaged later
                date = datetime.datetime.fromisoformat(entry["date"])
                value = entry["value"]
                station = entry["station"]

                new_entry = WeatherEntry(date=date, temperature=value, station=station)

                session.add(new_entry)
            
            session.commit()

        netzero.util.print_status("Weather", "Complete", newline=True)


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


class WeatherEntry(netzero.db.ModelBase):
    __tablename__ = "weather"

    date = Column(Date, primary_key=True)
    temperature = Column(Float)
    station = Column(String, primary_key=True)

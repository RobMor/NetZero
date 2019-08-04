"""Data source specification for Solar Edge data.

SolarEdge supplies an API with their solar panels which allows you to view
the power supplied by you panels in high detail. In this module we collect that
data and convert it into a useful daily summation of energy production.

Solar Edge API documentation (ca 2019):
https://www.solaredge.com/sites/default/files/se_monitoring_api.pdf
"""

import json
import requests
import datetime
import sqlite3

from netzero.sources import DataSource
from netzero.util import util


class Solar(DataSource):
    default_start = datetime.datetime(2016, 1, 27)
    default_end = datetime.datetime.today()

    def __init__(self, config):
        util.validate_config(config, entry="solar", fields=["api_key", "site_id"])

        self.api_key = config["solar"]["api_key"]
        self.site_id = config["solar"]["site_id"]

    def collect_data(self, start_date=None, end_date=None):
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

        # Iterate through each date range
        for interval in util.time_intervals(start_date, end_date, days=30):
            result = self.query_api(interval[0], interval[1])

            for entry in result["energy"]["values"]:
                # Parse the time from the given string
                date = datetime.datetime.fromisoformat(entry["date"])
                value = entry["value"] or 0  # 0 if None

                yield date, value

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
            "timeUnit": "QUARTER_OF_AN_HOUR"
        }
        data = requests.get("https://monitoringapi.solaredge.com/site/" +
                            self.site_id + "/energy.json",
                            params=payload)
        return json.loads(data.text)

    def process_data(self):
        """Computes the daily input of the solar panels in kWh."""
        # Developers note, these dates are in EST already so we can just directly
        # convert them
        return SolarAgg

def SolarAgg(object):
    def __init__(self):
        self.sum = 0
    
    def step(self, value):
        self.sum += value

    def finalize(self):
        return self.sum / 1000
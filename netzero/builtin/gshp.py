import sqlite3
import datetime
import json
import time

import requests
import bs4

from netzero.sources import DataSource
from netzero import util


class Gshp(DataSource):
    name = "GSHP"
    option = "g"
    long_option = "gshp"
    summary = "collects ground source heat pump data"
    columns = (DataSource.TIME, "value")

    default_start = datetime.datetime(2016, 10, 31)
    default_end = datetime.datetime.today()

    def __init__(self, config):
        util.validate_config(config,
                             entry="gshp",
                             fields=["username", "password"])

        self.username = config["gshp"]["username"]
        self.password = config["gshp"]["password"]

    def collect_data(self, start_date=None, end_date=None):
        """Collects raw furnace usage data from the Symphony website.

        Parameters
        ---------
        start_date : datetime.date, optional
            The start of the time interval to collect data for
        end_date : datetime.date, optional
            The end of the time interval to collect data for
        """
        if start_date is None:
            start_date = self.default_start
        if end_date is None:
            end_date = self.default_end

        # Move the start date back one day
        start_date = start_date - datetime.timedelta(days=1)

        session = self.establish_session()

        for _, day in util.time_intervals(start_date, end_date, days=1):
            parsed = self.scrape_json(session, day)

            for row in parsed:
                time = int(row["1"])  # Unix timestamp
                time = datetime.datetime.fromtimestamp(time)

                value = int(row["78"])  # The number of Watts

                yield time, value

        session.close()

    def establish_session(self) -> requests.Session:
        """Establishes a session with the symphony website 
        
        Establishes a session and logs in using the users credentials.
        Then navigates to the historical data page where we can collect data.
        """
        # Payload for the request to log in
        payload = {
            "op": "login",
            "redirect": "/",
            "emailaddress": self.username,
            "password": self.password
        }

        s = requests.Session()

        # Allow the session to retry a connection up to 5 times
        # The GSHP website is flaky
        retry_adapter = requests.adapters.HTTPAdapter(max_retries=5)
        s.mount("http://", retry_adapter)
        s.mount("https://", retry_adapter)

        # Login to the site
        p = s.post("https://symphony.mywaterfurnace.com/account/login",
                   data=payload)

        # Find the tokens that seem to be necessary for the next few steps
        soup = bs4.BeautifulSoup(p.text, "html.parser")
        field = soup.find("a", attrs={
            "title": "AWL Tech View"
        }).attrs["href"][1:]  # Get everything except the /

        # Navigate some more
        # Navigating here allows us to actually collect the data.
        # Necessary in order the query the fetch.php script.
        s.get("https://symphony.mywaterfurnace.com/dealer/historical-data" +
              field)

        return s

    def scrape_json(self, session, date):
        """Requests some data for a certain day from the Symphony website.

        Parameters
        ----------
        s : requests.Session
            Active session (from establish_session)
        date : datetime.date
            Date to get the data for
        
        Returns
        -------
        A python list containing the data in the format:
            [
                {
                    "1":_,
                    "2":_, <-- This is the time of the entry
                    ...
                    "78":_, <-- This is the energy usage (in Watts) for the entry
                    ...
                }
            ]
        Every value in the JSON objects is a string
        """
        params = {"json": '', "date": date.strftime("%m-%d-%Y")}
        # Putting the date you want information for after this url returns some
        # json containing all the data for that day.
        # Found with some simple network analysis using browser tools...
        response = session.get("https://symphony.mywaterfurnace.com/fetch.php",
                               params=params)

        if response.ok:
            return response.json()
        else:
            return []

    def aggregators(self):
        """
        Calculates the daily energy usage by the Ground Source Heat Pump system
        based on the readings. This means you have to multiply each time interval by
        the amount of power it was using, and then sum this inervals up over the day
        to get the entire power usage for the day.
        """
        return {("time", "value"): WattHourAgg}


### TODO -- Deal with missing data. Hours at a time may be unaccounted for!!!
class WattHourAgg(object):
    """An Sqlite3 aggregator to convert GSHP power usage to energy usage

    This class is meant to be fed to sqlite3's create_aggregate method. The 
    constructor defines the beginning state, before any entries have been read. 
    The step method handles new entries. Finally the finalize method returns 
    whatever the final result is.
    """
    def __init__(self):
        """
        Initialize values
        """
        self.watt_hours = 0
        self.prev_time = None

    def step(self, time, value):
        """
        Read in sorted timeseries data. Compute the amount of time since the
        previous entry and conert it to hours. Convert the watts to kilowatts 
        and multiply the time and the wattage to get the energy usage.
        """
        time = datetime.datetime.fromisoformat(time)
        kw = value / 1000

        if self.prev_time:  # Compute time since previous entry
            h = (time - self.prev_time).total_seconds() / 3600
        else:  # Compute time since start of day
            midnight = time.replace(hour=0, minute=0, second=0)
            h = (time - midnight).total_seconds() / 3600

        self.watt_hours += kw * h

        self.prev_time = time

    def finalize(self):
        return self.watt_hours

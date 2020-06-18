import datetime
import json
import os
import sqlite3

import bs4
import requests

from netzero.sources import SourceBase
import netzero.util


class Gshp(SourceBase):
    name = "gshp"
    summary = "Symphony ground source heat pump data"

    default_start = datetime.date(2016, 10, 31)
    default_end = datetime.date.today()

    def __init__(self, config, conn):
        config = netzero.util.validate_config(
            config, entry="GSHP", fields=["username", "password"]
        )

        self.username = config["username"]
        self.password = config["password"]

        self.conn = conn
        self.conn.create_aggregate("WATTAGG", 2, WattHourAgg)

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS gshp (time TIMESTAMP PRIMARY KEY, watts FLOAT)"
        )

    def collect(self, start_date=None, end_date=None):
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

        cur = self.conn.cursor()

        # Move the start date back one day
        start_date = start_date - datetime.timedelta(days=1)

        session = self.establish_session()

        total = (end_date - start_date).days

        self.reset_status("GSHP", "Collecting Data", total)

        for i, (_, day) in enumerate(netzero.util.time_intervals(start_date, end_date, days=1)):
            self.set_progress(day.strftime("%Y-%m-%d"), i)

            parsed = self.scrape_json(session, day)

            if len(parsed) > 0:
                start = datetime.datetime.fromtimestamp(int(parsed[0]["1"]))
                end = datetime.datetime.fromtimestamp(int(parsed[-1]["1"]))
            else:
                start = day
                end = day

            for row in parsed:
                time = int(row["1"])  # Unix timestamp
                time = datetime.datetime.fromtimestamp(time)

                value = int(row["78"])  # The number of Watts

                cur.execute("INSERT OR IGNORE INTO gshp VALUES (?, ?)", (time, value))

            self.conn.commit()

        cur.close()
        session.close()

        self.set_progress("Complete", total)

    def establish_session(self) -> requests.Session:
        """Establishes a session with the symphony website 
        
        Establishes a session and logs in using the users credentials.
        Then navigates to the historical data page where we can collect data.
        """
        self.reset_status("GSHP", "Establishing Session", 2)

        # Payload for the request to log in
        payload = {
            "op": "login",
            "redirect": "/",
            "emailaddress": self.username,
            "password": self.password,
        }

        s = requests.Session()

        # Allow the session to retry a connection up to 5 times
        # The GSHP website is flaky
        retry_adapter = requests.adapters.HTTPAdapter(max_retries=5)
        s.mount("http://", retry_adapter)
        s.mount("https://", retry_adapter)

        self.set_progress("Logging In", 0)

        # Login to the site
        p = s.post("https://symphony.mywaterfurnace.com/account/login", data=payload)

        # Find the tokens that seem to be necessary for the next few steps
        soup = bs4.BeautifulSoup(p.text, "html.parser")
        # Get everything except the /
        field = soup.find("a", attrs={"title": "AWL Tech View"}).attrs["href"][1:]

        self.set_progress("Preparing Data", 1)

        # Navigate some more
        # Navigating here allows us to actually collect the data.
        # Necessary in order the query the fetch.php script.
        s.get("https://symphony.mywaterfurnace.com/dealer/historical-data" + field)

        self.set_progress("Session Established", 2)

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
        params = {"json": "", "date": date.strftime("%m-%d-%Y")}
        # Putting the date you want information for after this url returns some
        # json containing all the data for that day.
        # Found with some simple network analysis using browser tools...
        response = session.get(
            "https://symphony.mywaterfurnace.com/fetch.php", params=params
        )

        if response.ok:
            return response.json()
        else:
            return []

    def min_date(self):
        result = self.conn.execute("SELECT date(min(time)) FROM gshp").fetchone()[0]

        return datetime.datetime.strptime(result, "%Y-%m-%d").date()

    def max_date(self):
        result = self.conn.execute("SELECT date(max(time)) FROM gshp").fetchone()[0]

        return datetime.datetime.strptime(result, "%Y-%m-%d").date()

    def format(self):
        netzero.util.print_status("GSHP", "Querying Database")

        data = self.conn.execute(
            "SELECT date(time), WATTAGG(time, watts) FROM gshp GROUP BY date(time)"
        ).fetchall()

        result = {
            datetime.datetime.strptime(date, "%Y-%m-%d").date(): value
            for date, value in data
        }

        netzero.util.print_status("GSHP", "Complete", newline=True)

        return result


# TODO -- Deal with missing data. Hours at a time may be unaccounted for!!!
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

        if self.prev_time is not None:  # Compute time since previous entry
            h = (time - self.prev_time).total_seconds() / 3600
        else:  # Compute time since start of day
            midnight = time.replace(hour=0, minute=0, second=0)
            h = (time - midnight).total_seconds() / 3600

        self.watt_hours += kw * h

        self.prev_time = time

    def finalize(self):
        return self.watt_hours

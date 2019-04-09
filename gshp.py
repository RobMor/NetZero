import sqlite3, datetime, json, time, progressbar, requests
from bs4 import BeautifulSoup as bsoup


def get_data(conn, username, password, 
    start_date=datetime.datetime(2016, 10, 31), 
    end_date=datetime.datetime.today()):
    """
    Handles everything in the process of mining the GSHP data.

    :param conn: An sqlite database connection
    :param username: The users Symphony username
    :param password: The users Symphony password
    :param start_date: The start of the time interval to collect data for
    :param end_date: The end of the time interval to collect data for
    """

    cur = conn.cursor()

    # Make sure that the gshp_raw table is already in the database
    cur.execute("""SELECT name FROM sqlite_master 
    WHERE type='table' and name='gshp_raw'""")
    if not cur.fetchall(): # Table isn't in database
        cur.execute("""
        CREATE TABLE gshp_raw(
            time INTEGER PRIMARY KEY,
            value INTEGER
        )""")
        conn.commit()

    # Make sure that the gshp_day table is already in the database
    cur.execute("""SELECT name FROM sqlite_master 
    WHERE type='table' and name='gshp_day'""")
    if not cur.fetchall(): # Table isn't in database
        cur.execute("""
        CREATE TABLE gshp_day(
            day TEXT PRIMARY KEY,
            value INTEGER
        )""")
        conn.commit()
    
    # Mine the data from the GSHP website
    store_raw_data(conn, username, password, start_date, end_date)

    # Calculate the daily energy usage
    calculate_daily(conn)


# Establishes a session with the GSHP website. First it logs in, then navigates to the historical data section of the website
def establish_session(username, password):
    """
    Establishes a session with the symphony website using the users credentials
    and navigates to the historical data page where we can collect data.
    """
    # Payload for the request to log in
    payload = {
        "op": "login",
        "redirect": "/",
        "emailaddress": username,
        "password": password
    }

    s = requests.Session()

    print("Attempting Login")
    # Login to the site
    p = s.post("https://symphony.mywaterfurnace.com/account/login",
               data=payload)
    print("Login Success")

    # Find the tokens that seem to be necessary for the next few steps
    soup = bsoup(p.text, "html.parser")
    field = soup.find("a", attrs={"title": "AWL Tech View"}).attrs["href"][1:] # Get everything except the /

    # Navigate some more
    s.get("https://symphony.mywaterfurnace.com/dealer/historical-data" + field)

    return s


def get_json(date, s):
    """
    Requests some data for a certain day from the Symphony website.

    :param date: Date to get the data for
    :param s: Active session (from establish_session)
    :returns: A python list containing the data in the format:
        [
            {
                "1":_,
                "2":_, <-- This is the time of the entry
                ...
                "78":_, <-- This is the energy usage (in Watts) for the entry
                ...
            }
        ]
    """
    # Putting the date you want information for after this url returns some
    # json containing all the data for that day.
    # Found with some simple network analysis using browser tools...
    data = s.get("https://symphony.mywaterfurnace.com/fetch.php?json&date=" +
                 date.strftime("%m-%d-%Y"))
    try:
        parsed = json.loads(data.text)
    except json.decoder.JSONDecodeError:
        return []
    else:
        return parsed


def day_range(start_date, end_date):
    """
    Returns a list of datetimes containing the days between (inclusive) stard
    and end date
    :param start_date: The start of the time interval
    :param end_date: The end of the time interval
    :returns: A list of datetimes containing all the dates between start and end

    TODO -- May not account for daylight savings time correctly
    """
    num_days = (end_date - start_date).days

    return [start_date + datetime.timedelta(days=d) for d in range(0, num_days+1)]


widgets = ["GSHP: Downloading ", progressbar.SimpleProgress(), progressbar.Bar(), " ", progressbar.ETA()]

retry_on = (  # Exceptions to retry the connection on
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    requests.exceptions.HTTPError
)

# Actually use the internet to get the data from the site.
def store_raw_data(conn, username, password, start_date, end_date, num_retries=10):
    """
    Collects the raw data from the Symphony website 
    (https://symphony.mywaterfurnace.com/) and stores it in the database.

    :param conn: An sqlite database connection
    :param username: The users Symphony username
    :param password: The users Symphony password
    :param start_date: The start of the time interval to collect data for
    :param end_date: The end of the time interval to collect data for
    """
    cur = conn.cursor()

    # THIS IS A SAFEGUARD BECAUSE THE WEBSITE IS PRETTY FLAKY
    # tries its best to keep going...
    for _ in range(0, num_retries):
        try:
            # This data collection step prints a lot more than the others
            # because the collection takes so long that we want to keep the
            # users informed so they know it's still working at a glance...
            print("Starting Session")
            s = establish_session(username, password)

            days = day_range(start_date, end_date)
            bar = progressbar.progressbar(days, widgets=widgets, redirect_stdout=True)

            print("Beginning Data Collection")
            # Finally get the data
            for date in bar:
                print("Collecting Data For:", date.strftime("%Y-%m-%d"))
                parsed = get_json(date, s)
                print("Completed:", date.strftime("%Y-%m-%d"))

                for row in parsed:
                    time = row["1"] # Unix timestamp
                    value = row["78"] # The number of Watts used in the time frame

                    cur.execute("""INSERT OR IGNORE INTO gshp_raw(time, value) 
                        VALUES(?,?)""", (time, value))

                conn.commit()

                # Mark our progress in case of error
                start_date = date

            print("All Data Collected")
            s.close()
            return

        except retry_on: # THIS HAPPENS WHEN THE SITE DIES
            print("Network Issue, Retrying in 5 Seconds...")
            time.sleep(5)
            continue
    else:
        raise Exception("Network Failure! Try Again Later.")


class WattHourAgg:
    """
    A class in the format specified by sqlite so it is accepted by the
    create_aggregate method. The constructor defines the beginning state, before
    any entries have been read. The step method handles new entries. Finally the
    finalize method returns whatever the final result is.
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
        kw = value / 1000

        if self.prev_time: # Compute time since previous entry
            h = (time - self.prev_time) / 3600
        else: # Compute time since start of day
            timestamp = datetime.datetime.fromtimestamp(time)
            midnight = timestamp.replace(hour=0, minute=0, second=0)
            h = (timestamp - midnight).total_seconds() / 3600

        self.watt_hours += kw * h

        self.prev_time = time

    def finalize(self):
        return self.watt_hours

### TODO -- Deal with missing data. Hours at a time may be unaccounted for!!!

def calculate_daily(conn):
    """
    Calculates the daily energy usage by the Ground Source Heat Pump system
    based on the readings. This means you have to multiply each time interval by
    the amount of power it was using, and then sum this inervals up over the day
    to get the entire power usage for the day.
    """
    cur = conn.cursor()

    cur.execute("DELETE FROM gshp_day")

    # Utilize sqlites custom aggregation functions
    conn.create_aggregate("WATTHOURS", 2, WattHourAgg)

    # Make sure to sort, and THEN group by the day.
    # The aggregation function depends on the data being sorted to work properly
    cur.execute("""
    INSERT OR IGNORE INTO gshp_day
    SELECT 
        DATE(time, 'unixepoch') AS day,
        WATTHOURS(time, value)
    FROM 
        (SELECT time, value from gshp_raw ORDER BY time) AS sorted
    GROUP BY day
    """)

    conn.commit()
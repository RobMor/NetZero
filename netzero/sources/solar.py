import json, requests, datetime, sqlite3, progressbar


def get_data(conn, site, api_key, start_date=datetime.datetime(2016, 1, 27), end_date=datetime.datetime.today()):
    """
    Orchestrates the collection of solar data from Solar Edge

    :param conn: An sqlite database connection
    :param site: The siteID to retrieve data from
    :param api_key: The api key to access Solar Edges API
    :param start: The start of the time interval to collect data from
    :param end: The end of the time interval to collect data from
    """
    cur = conn.cursor()

    # Make sure that the solar_raw table is already in the database
    cur.execute("""SELECT name FROM sqlite_master 
    WHERE type='table' and name='solar_raw'""")
    if not cur.fetchall(): # Table isn't in database
        cur.execute("""
        CREATE TABLE solar_raw(
            time TEXT PRIMARY KEY,
            value INTEGER
        )""")
        conn.commit()

    # Make sure that the solar_day table is already in the database
    cur.execute("""SELECT name FROM sqlite_master 
    WHERE type='table' and name='solar_day'""")
    if not cur.fetchall(): # Table isn't in database
        cur.execute("""
        CREATE TABLE solar_day(
            day TEXT PRIMARY KEY,
            value INTEGER NOT NULL
        )""")
        conn.commit()

    # Collect the raw data
    store_raw_data(conn, site, api_key, start_date, end_date)

    # Calculate daily usage and store in a database
    calculate_daily(conn)


def query_api(key, site, span):
    """
    A method to query the Solar Edge api for energy data

    :param key: The api_key to be fed to the API, given to the user by SolarEdge
    :param site: The Site ID to collect the data from
    :param span: The span of time to query the API for (must be less than a month)
    :returns: The API response in python dict format:
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
        "api_key": key,
        "startDate": span[0].strftime("%Y-%m-%d"),
        "endDate": span[1].strftime("%Y-%m-%d"),
        "timeUnit": "QUARTER_OF_AN_HOUR"
        # Even though we condense this data down to a daily sum we still want to
        # collect as much data as possible because perhaps it may some day be
        # useful.
    }
    data = requests.get("https://monitoringapi.solaredge.com/site/"+site+"/energy.json", params=payload)
    return json.loads(data.text)


def monthly_intervals(start_date, end_date):
    """
    Computes a list of tuples (datetime, datetime) that represent month long
    intervals of time to most compactly collect all the solar data we possibly
    can. The SolarEdge API only offers quarter of an hour granularity on a
    monthly interval limit.
    """
    month = datetime.timedelta(days=30)

    intervals = []
    prev = start_date
    while (prev + month) < end_date:
        intervals.append((prev, prev+month))
        prev = prev+month

    intervals.append((prev, end_date))

    return intervals


widgets = ["Solar: Downloading ", progressbar.SimpleProgress(), progressbar.Bar(), " ", progressbar.ETA()]

def store_raw_data(conn, site, api_key, start_date, end_date):
    """
    Collects and stores the raw data from the SolarEdge API in our database.

    :param conn: An sqlite database connection
    :param site: The SolarEdge site id to collect data for
    :param api_key: The SolarEdge API key provided by them
    :param start_date: The start of the time interval to collect data on
    :
    """
    cur = conn.cursor()
    
    # Create a range of month long intervals
    spans = monthly_intervals(start_date, end_date)
    
    # Create a visual progress bar.
    bar = progressbar.progressbar(spans , widgets=widgets, redirect_stdout=True)

    # Iterate through each date range
    for span in bar:
        result = query_api(api_key, site, span)

        for entry in result["energy"]["values"]:
            # Parse the time from the given string
            date = entry["date"] #Arrow.strptime(entry["date"], "%Y-%m-%d %X", tzinfo="EST").timestamp
            value = entry["value"] or 0  # 0 if None

            cur.execute("""INSERT OR IGNORE INTO solar_raw(time, value)
                VALUES(?,?)""", (date, value))
    
    conn.commit()


def calculate_daily(conn):
    """
    Computes the daily input of the solar panels in kWh.
    """
    cur = conn.cursor()

    # Developers note, these dates are in EST already so we can just directly
    # convert them
    cur.execute("""
    INSERT OR IGNORE INTO solar_day 
    SELECT 
        DATE(time, 'start of day') AS day,
        SUM(value) / 1000.0 
    FROM solar_raw GROUP BY day
    """)

    conn.commit()
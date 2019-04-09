import sqlite3, datetime, requests, progressbar


def get_data(conn, api_token, 
             start_date=datetime.datetime(2014, 1, 1),
             end_date=datetime.datetime.today()):
    """
    Handles weather data mining.
    """
    cur = conn.cursor()

    # Make sure that the weather_raw table is already in the database
    cur.execute("""SELECT name FROM sqlite_master 
    WHERE type='table' and name='weather_raw'""")
    if not cur.fetchall(): # Table isn't in database
        # Day is not primary key because we average across 3 different stations
        cur.execute("""
        CREATE TABLE weather_raw(
            day TEXT,
            value INTEGER
        )""")
        conn.commit()

    # Make sure that the solar_raw table is already in the database
    cur.execute("""SELECT name FROM sqlite_master 
    WHERE type='table' and name='weather_day'""")
    if not cur.fetchall(): # Table isn't in database
        cur.execute("""
        CREATE TABLE weather_day(
            day TEXT PRIMARY KEY,
            value INTEGER
        )""")
        conn.commit()

    # Collect the raw weather data from the API
    store_raw_data(conn, api_token, start_date, end_date)

    # Consolidate the weather data in to daily averages
    calculate_daily(conn)


stations = ["GHCND:USW00093721", "GHCND:USW00093738"]  # Stations: [BWI, Dulles]

def query_api(api_token, start_date, end_date):
    """
    Query the NCDC API for average daily temperature data for a given time span

    :param api_token: API Token handed out by NOAA
    :param start_date: Start of time range to collect data for
    :param end_date: End of time range to collect data for
    :returns: A python dict containing the data in the format:
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
        "token": api_token
    }
    params = {
        "datasetid": "GHCND",  # Daily weather
        "stationid": stations, 
        "datatypeid": "TAVG",  # Average Temperature
        "units": "standard",  # Fahrenheit
        "limit": 1000,  # Maximum request size
        "startdate": start_date.strftime("%Y-%m-%d"),
        "enddate": end_date.strftime("%Y-%m-%d")
    }   

    response = requests.get("https://www.ncdc.noaa.gov/cdo-web/api/v2/data", headers=headers, params=params)
    try:
        return response.json()
    except ValueError:
        print("Error Decoding Weather Data")
        print(response.text)
        return {}


def intervals(num_stations, start_date, end_date):
    """
    Compute time intervals so that the API returns less than 1000 entries on each

    :param num_stations: The number of stations being queried
    :param start_date: The start of the time interval to break up
    :param end_date: The end of the time interval to break up
    :returns: A list of tuples, representing the time intervals
    """
    days = int(1000/num_stations)

    delta = datetime.timedelta(days=days)

    intervals = []
    prev = start_date
    while (prev + delta) < end_date:
        intervals.append((prev, (prev + delta)))
        prev = prev + delta

    intervals.append((prev, end_date))

    return intervals


widgets = ["Weather: Downloading ", progressbar.SimpleProgress(), progressbar.Bar(), " ", progressbar.ETA()]

def store_raw_data(conn, api_token, start_date, end_date):
    '''
    Gets weather data from NOAAs ncdc API service.

    :param api_token: The NCDC API token to use
    :param start_date: The start of the time frame to collect data on
    :param end_date: The end of the time frame to collect data on
    :return: Dict: date -> average temp
    '''
    cur = conn.cursor()

    # Generate year long time intervals from start to end date
    spans = intervals(len(stations), start_date, end_date)

    # Create a visual progressbar
    bar = progressbar.progressbar(spans, widgets=widgets, redirect_stdout=True)

    for span in bar:
        # TODO -- REMOVE ASSUMPTION THAT LEN(DATA) < LIMIT
        raw_data = query_api(api_token, span[0], span[1])
        
        if "results" in raw_data:
            for entry in raw_data["results"]:
                # Insert the weather data to the table, to be averaged later
                day = entry["date"]
                val = entry["value"]

                cur.execute("""INSERT INTO weather_raw(day, value) 
                    VALUES(?,?)""", (day, val))

        conn.commit()


def calculate_daily(conn):
    """
    Computes average daily temperatures based on the readings of each station.
    """
    cur = conn.cursor()

    cur.execute("""
    INSERT OR IGNORE INTO weather_day
    SELECT
        DATE(day, 'unixepoch') as realday,
        AVG(value)
    FROM weather_raw GROUP BY realday
    """)

    conn.commit()

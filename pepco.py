import sqlite3, itertools, progressbar
import xml.etree.ElementTree as ETree # Used to traverse XML


def get_data(conn, files):
    """
    Starts the process of collecting data on pepco

    :param conn: An sqlite database connection
    :param files: A list of files containing the Pepco data to process.
    """
    cur = conn.cursor()

    # Make sure that the pepco_raw table is already in the database
    cur.execute("""SELECT name FROM sqlite_master 
    WHERE type='table' and name='pepco_raw'""")
    if not cur.fetchall(): # Table isn't in database
        cur.execute("""
        CREATE TABLE pepco_raw(
            time INTEGER PRIMARY KEY,
            value INTEGER NOT NULL
        )""")
        conn.commit()

    # Make sure that the pepco_day table is already in the database
    cur.execute("""SELECT name FROM sqlite_master
    WHERE type='table' and name='pepco_day'""")
    if not cur.fetchall():
        cur.execute("""CREATE TABLE pepco_day(
            day TEXT PRIMARY KEY,
            value REAL NOT NULL
        )""")

    # Collect the raw data
    store_raw_data(conn, files)

    # Calculate daily usage and store in a database
    calculate_daily(conn)

    
def combine_files(files):
    '''
    Combines the list of entry files for any number of Green Button data files.

    :param files: A list of file names to combine
    :return: An iterator for each of the entry tags
    '''
    entries = []
    for file in files:
        with open(file) as f:
            tree = ETree.parse(f)
        root = tree.getroot()
        # Chain together the previous set of entries to the next set
        entries = itertools.chain(entries, root.findall(tags["entry"]))

    return entries


"""
Green Button XML Format for Documentation Purposes:

<feed>
    <id></id>
    <title></title>
    <updated></updated>
    <entry>
        <id></id>
        # We only want entry tags with this title.
        <title>Energy Usage</title>
        <content>
            <IntervalBlock>
                <interval> # Only one of these.
                    <duration>[unix timestamp]</duration>
                    <start>[unix timestamp]</start>
                </interval>
                <IntervalReading>
                    <timePeriod>
                        <duration>[unix timestamp]</duration>
                        <start>[unix timestamp]</start>
                    </timePeriod>
                    <value>[value in Wh]</value>
                </IntervalReading>
                ...
            </IntervalBlock>
        </content>
    </entry>
    ...
</feed>
"""

# Relevant tags for the Green Button data format
tags = {
    "entry": "{http://www.w3.org/2005/Atom}entry",
    "title": "{http://www.w3.org/2005/Atom}title",
    "content": "{http://www.w3.org/2005/Atom}content",
    "IntervalBlock": "{http://naesb.org/espi}IntervalBlock",
    "interval": "{http://naesb.org/espi}interval",
    "IntervalReading": "{http://naesb.org/espi}IntervalReading",
    "start": "{http://naesb.org/espi}start",
    "value": "{http://naesb.org/espi}value",
    "timePeriod": "{http://naesb.org/espi}timePeriod",
}

widgets = ["Pepco: Parsing ", progressbar.SimpleProgress(), progressbar.Bar(), " ", progressbar.ETA()]

def store_raw_data(conn, files):
    """
    Collect the raw energy usage data from Pepco's XML files and store it in the
    database.

    :param conn: An sqlite database connection
    :param files: A list of Pepco XML files containing all the data.
    """
    cur = conn.cursor()

    # Combine files into one long list of entries.
    entries = combine_files(files)

    # Create a visual progressbar for our iteration
    bar = progressbar.progressbar(entries, widgets=widgets, redirect_stdout=True)

    # Iterate through each entry
    for entry in bar:
        # Find the text within the title tag
        title = entry.find(tags["title"]).text

        # Only care about entries titled "Energy Usage"
        if title == "Energy Usage":
            # Traverse structure
            block = entry.find(tags["content"]).find(tags["IntervalBlock"])
            
            # Iterate through the readings, storing each one in the database
            for reading in block.findall(tags["IntervalReading"]):
                # Read start time and usage in Wh from XML file
                start = int(reading.find(tags["timePeriod"]).\
                    find(tags["start"]).text)
                value = int(reading.find(tags["value"]).text)

                cur.execute("""INSERT OR IGNORE INTO pepco_raw(time, value) 
                    VALUES(?,?)""", (start, value))

    # Commit the data to the database
    conn.commit()


def calculate_daily(conn):
    """
    Calculates the daily power usage in kWh for each day in pepco_raw
    
    :param conn: An sqlite database connection
    """
    cur = conn.cursor()

    # Developers note, these dates are in EST already so we can just directly
    # convert them
    cur.execute("""
    INSERT OR IGNORE INTO pepco_day 
    SELECT 
        DATE(time, 'unixepoch') AS day,
        SUM(value) / 1000.0 
    FROM pepco_raw GROUP BY day
    """)

    conn.commit()
"""Data Source Specification for Pepco data.

Pepco data comes in the format of a 'green-button' xml file. This file contains
detailed information on energy usage over time. Here we parse that information
out and convert it into a useful daily summation of energy used.


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

import sqlite3
import datetime
import itertools
import xml.etree.ElementTree as ETree

from netzero.sources.base import DataSource


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


class Pepco(DataSource):
    default_start = None
    default_end = None

    def __init__(self, config, conn):
        super().validate_config(config, entry="pepco", fields=["files"])

        self.files = config["pepco"]["files"]

        self.conn = conn

        with self.conn:
            # Create the table for the raw data
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS pepco_raw(time TIMESTAMP PRIMARY KEY, value REAL)
            """)
            
            # Create the table for the processed data
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS pepco_day(date TIMESTAMP PRIMARY KEY, value REAL)
            """)

    def collect_data(self, start_date=None, end_date=None) -> None:
        """Collects data from PEPCO XML files.

        Collects the raw energy usage data from Pepco's XML files and stores it
        in the database.

        Parameters
        ----------
        start : datetime.datetime, optional
            The start of the data collection range
        end : datetime.datetime, optional
            The end of the data collection range
        """
        # Combine files into one long list of entries.
        entries = self.concatenate_files(self.files)

        # Iterate through each entry
        for entry in entries:
            # Find the IntervalBlock tag underneath the content tag
            content = entry.find(tags["content"])
            block = None
            if content:
                block = content.find(tags["IntervalBlock"])

            # Only care about entries with IntervalBlock tags
            if block is not None:
                with self.conn:
                    # Iterate through the readings, storing each one in the database
                    for reading in block.findall(tags["IntervalReading"]):
                        # Read start time and usage in Wh from XML file
                        start = int(reading.find(tags["timePeriod"]).find(tags["start"]).text)
                        start = datetime.datetime.fromtimestamp(start).strftime(r"%Y-%m-%d %H:%M:%S")

                        value = int(reading.find(tags["value"]).text)

                        self.conn.execute("""
                            INSERT OR IGNORE INTO pepco_raw(time, value) 
                            VALUES(?,?)
                        """, (start, value))

                        print("PEPCO:", start, "--", value)

    def concatenate_files(self, files):
        """
        Combines the list of entry files for any number of Green Button data files.

        :param files: A list of file names to combine
        :return: An iterator for each of the entry tags
        """
        entries = []
        for file in files:
            with open(file) as f:
                tree = ETree.parse(f)
            root = tree.getroot()
            # Chain together the previous set of entries to the next set
            entries = itertools.chain(entries, root.findall(tags["entry"]))

        return entries

    def process_data(self) -> None:
        """Processes the PEPCO data in the database.

        Calculates the daily power usage in kWh for each day in pepco_raw.

        Parameters
        ----------
        start : datetime.datetime.DateTime, optional
            The start of the data collection range
        end : datetime.datetime.DateTime, optional
            The end of the data collection range
        """
        with self.conn:
            self.conn.execute("""
                INSERT OR IGNORE INTO pepco_day 
                SELECT 
                    DATE(time) AS day,
                    SUM(value) / 1000.0
                FROM pepco_raw GROUP BY day
            """)

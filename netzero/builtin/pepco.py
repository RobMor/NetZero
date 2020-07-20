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

import datetime
import itertools
import json
import os
import sqlite3
import xml.etree.ElementTree as ETree

import netzero.util

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


class Pepco:
    name = "pepco"
    summary = "Pepco data"

    def __init__(self, config, database):
        netzero.util.validate_config(config, entry="pepco", fields=["files"])

        self.files = json.loads(config["pepco"]["files"])

        self.conn = sqlite3.connect(database)

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS pepco (time TIMESTAMP PRIMARY KEY, watt_hrs FLOAT)"
        )

    def collect(self, start_date=None, end_date=None) -> None:
        """Collects data from PEPCO XML files.

        Collects the raw energy usage data from Pepco's XML files and stores it
        in the database.

        Parameters
        ----------
        start : datetime.date, optional
            The start of the data collection range
        end : datetime.date, optional
            The end of the data collection range
        """
        cur = self.conn.cursor()
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
                # Iterate through the readings, storing each one in the database
                for reading in block.findall(tags["IntervalReading"]):
                    # Read start time and usage in Wh from XML file
                    start = int(
                        reading.find(tags["timePeriod"]).find(tags["start"]).text
                    )
                    start = datetime.datetime.fromtimestamp(start)

                    value = int(reading.find(tags["value"]).text)

                    cur.execute(
                        "INSERT OR IGNORE INTO pepco VALUES (?, ?)", (start, value)
                    )

                self.conn.commit()

        cur.close()

    def concatenate_files(self, files):
        """
        Generates entries from each of the provided Greenbutton data files.

        :yields: 
        """
        entries = []
        for f in files:
            tree = ETree.parse(f)

            root = tree.getroot()
            entries = root.findall(tags["entry"])

            for entry in entries:
                yield entry

    def min_date(self):
        result = self.conn.execute("SELECT date(min(time)) FROM pepco").fetchone()[0]

        if result is None:
            return None
        else:
            return datetime.datetime.strptime(result, "%Y-%m-%d").date()

    def max_date(self):
        result = self.conn.execute("SELECT date(max(time)) FROM pepco").fetchone()[0]

        if result is None:
            return None
        else:
            return datetime.datetime.strptime(result, "%Y-%m-%d").date()

    def format(self, start_date, end_date):
        netzero.util.print_status("Pepco", "Querying Database", newline=True)

        data = self.conn.execute(
            """
            WITH RECURSIVE
                range(d) AS (
                    SELECT date(?)
                    UNION ALL
                    SELECT date(d, '+1 day')
                    FROM range
                    WHERE range.d <= date(?)
                ),
                data(d, v) AS (
                    SELECT date(time), SUM(watt_hrs) / 1000
                    FROM pepco
                    GROUP BY date(time)
                )
            SELECT v FROM range NATURAL LEFT JOIN data""",
            (start_date, end_date),
        )

        netzero.util.print_status("Pepco", "Complete", newline=True)

        return data

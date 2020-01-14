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

from sqlalchemy import Column, DateTime, Float

import netzero.db
import netzero.util


class Solar:
    name = "solaredge"
    summary = "Solar Edge data"

    default_start = datetime.date(2016, 1, 27)
    default_end = datetime.date.today()


    def __init__(self, config):
        netzero.util.validate_config(config,
                             entry="solar",
                             fields=["api_key", "site_id"])

        self.api_key = config["solar"]["api_key"]
        self.site_id = config["solar"]["site_id"]


    def collect(self, session, start_date=None, end_date=None):
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
        for interval in netzero.util.time_intervals(start_date, end_date, days=30):
            netzero.util.print_status("SolarEdge", "Collecting: {} to {}".format(interval[0].isoformat(), interval[1].isoformat()))

            result = self.query_api(interval[0], interval[1])

            # Delete all previous entries for this time range
            # This is faster than merging everything which is what you have to
            # do because sqlalchemy doesnt support INSERT OR IGNORE.
            session.query(SolarEdgeEntry).filter(
                SolarEdgeEntry.time.between(
                    interval[0], 
                    interval[1] + datetime.timedelta(days=1),
                )
            ).delete(synchronize_session=False)
            
            for entry in result["energy"]["values"]:
                date = datetime.datetime.fromisoformat(entry["date"])
                value = entry["value"] or 0

                new_entry = SolarEdgeEntry(time=date, watt_hrs=value)

                session.add(new_entry)
            
            session.commit()
        
        netzero.util.print_status("SolarEdge", "Complete", newline=True)


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


class SolarEdgeEntry(netzero.db.ModelBase):
    __tablename__ = "solaredge"

    time = Column(DateTime, primary_key=True)
    watt_hrs = Column(Float)

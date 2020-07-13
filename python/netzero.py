import os
import io
import sys
import json
import datetime
import threading

class NetZeroApp:
    def __init__(self, collect, export):
        self.collect = collect
        self.export = export

    
    def start(self):
        """Start the app""" # TODO
        purpose = os.environ["PURPOSE"]

        if purpose == "collect":
            self._collect()
        elif purpose == "export":
            self._export()
        else:
            raise ValueError("Unrecognized Purpose")


    def _collect(self):
        start_date = os.getenv("START_DATE")
        if start_date is not None:
            start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")

        end_date = os.getenv("END_DATE")
        if end_date is not None:
            end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

        self.collect(self, start_date, end_date)

        self._send_msg({"type": "Done"})

    
    def _export(self):
        # TODO
        
        self.export(self)
    

    def _send_msg(self, message):
        sys.stdout.write(json.dumps(message) + "\n")
        sys.stdout.flush()

    
    def _send_progress_msg(self, message):
        self._send_msg({
            "type": "Progress",
            "message": message,
        })

    
    def send_set_max(self, value, status=None):
        self._send_progress_msg({
            "type": "SetMax",
            "max": int(value),
            "status": str(status) if status is not None else None,
        })


    def send_set_progress(self, progress, status=None):
        self._send_progress_msg({
            "type": "SetProgress",
            "progress": int(progress),
            "status": str(status) if status is not None else None,
        })


    def send_set_status(self, status):
        self._send_progress_msg({
            "type": "SetStatus",
            "status": str(status),
        })


    def send_reset(self):
        self._send_progress_msg({
            "type": "Reset",
        })


def bs(app, s, e):
    return


if __name__ == "__main__":
    NetZeroApp(bs, None).start()

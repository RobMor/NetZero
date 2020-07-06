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

        self._start_event = threading.Event()
        self._stop_event = threading.Event()

    
    def start(self):
        """Start the app""" # TODO
        purpose = os.environ["PURPOSE"]
        
        self._start_message_thread()

        if purpose == "collect":
            self._collect()
        elif purpose == "export":
            self._export()
        else:
            print("Unrecognized purpose", file=sys.stderr)


    def _collect(self):
        start_date = os.getenv("START_DATE")
        if start_date is not None:
            start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")

        end_date = os.getenv("END_DATE")
        if end_date is not None:
            end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        
        self._start_event.wait()
        self._send_msg({"type": "Starting"})

        self.collect(self, start_date, end_date)

        self._send_msg({"type": "Done"})
        sys.stdin.close()
        sys.stdout.close()

        self._message_thread.join()

        print("Main Thread Stopping", file=sys.stderr)

    
    def _export(self):
        # TODO
        
        self._start_event.wait()
        
        self.export(self)


    def _start_message_thread(self):
        def read_msg():
            while not self._stop_event.is_set():
                message = json.loads(next(sys.stdin))
                
                if message["type"] == "Start":
                    self._start_event.set()
                elif message["type"] == "Stop":
                    self._stop_event.set()
                    print("Message thread stopping", file=sys.stderr)
                    return

        self._message_thread = threading.Thread(target=read_msg)
        self._message_thread.start()


    def should_stop(self):
        return self._stop_event.is_set()
    
    
    def _send_msg(self, message):
        try:
            sys.stdout.write(json.dumps(message) + "\n")
            sys.stdout.flush()
        except Exception as e:
            print("ERROR SENDING:", e, file=sys.stderr)
            self._stop_event.set()

    
    def send_set_max(self, value, status=None):
        self._send_msg({
            "type": "SetMax",
            "value": int(value),
            "status": str(status) if status is not None else None,
            })


    def send_set_progress(self, progress, status=None):
        self._send_msg({
            "type": "SetProgress",
            "progress": int(progress),
            "status": str(status) if status is not None else None,
            })


    def send_set_status(self, status):
        self._send_msg({
            "type": "SetStatus",
            "status": str(status),
            })


    def send_reset(self):
        self._send_msg({
            "type": "Reset",
            })


def bs(app, s, e):
    return


if __name__ == "__main__":
    NetZeroApp(bs, None).start()

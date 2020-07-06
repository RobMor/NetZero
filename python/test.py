import time

from netzero import NetZeroApp


def collect(app, start_date, end_date):
    
    app.send_set_max(10);

    for i in range(10):
        if app.should_stop(): return
        app.send_set_progress(i, status="Doing thing {}".format(i))
        time.sleep(1)

    app.send_set_progress(10)


if __name__ == "__main__":
    NetZeroApp(collect, None).start()
    print("Done", file=sys.stderr)

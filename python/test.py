import time

from netzero import NetZeroApp


def collect(app, start_date, end_date):
    app.send_set_max(3);

    for i in range(3):
        app.send_set_progress(i, status="Doing thing {}".format(i))
        time.sleep(1)

    app.send_set_progress(3, status="Done")


if __name__ == "__main__":
    NetZeroApp(collect, None).start()

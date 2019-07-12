import datetime


def time_intervals(start_date, end_date, days=1, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0):
    delta = datetime.timedelta(days=days, seconds=seconds, microseconds=microseconds, milliseconds=milliseconds, minutes=minutes, hours=hours, weeks=weeks)

    intervals = []
    prev = start_date
    while (prev + delta) < end_date:
        yield (prev, prev+delta)
        prev = prev+delta

    yield (prev, end_date)
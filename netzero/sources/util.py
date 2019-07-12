import datetime


def time_intervals(start_date, end_date, **kwargs):
    delta = datetime.timedelta(**kwargs)

    intervals = []
    prev = start_date
    while (prev + delta) < end_date:
        yield (prev, prev+delta)
        prev = prev+delta

    yield (prev, end_date)
import datetime


def time_intervals(start_date,
                   end_date,
                   days=1,
                   seconds=0,
                   microseconds=0,
                   milliseconds=0,
                   minutes=0,
                   hours=0,
                   weeks=0):
    """A generator that produces inclusive time intervals from start to end date"""
    if start_date <= end_date:
        delta = datetime.timedelta(days=days,
                                   seconds=seconds,
                                   microseconds=microseconds,
                                   milliseconds=milliseconds,
                                   minutes=minutes,
                                   hours=hours,
                                   weeks=weeks)

        intervals = []
        prev = start_date
        while (prev + delta) < end_date:
            yield (prev, prev + delta)
            prev = prev + delta

        yield (prev, end_date)


def iter_days(start_date, end_date):
    assert isinstance(start_date, datetime.date)
    assert isinstance(end_date, datetime.date)

    if start_date < end_date:
        delta = datetime.timedelta(days=1)

        curr = start_date
        while curr < end_date:
            yield curr
            curr = curr + delta
        
        yield end_date


def validate_config(config, entry, fields):
    if entry not in config:
        raise ValueError("'%s' entry not in config" % entry)

    for field in fields:
        if field not in config[entry]:
            raise ValueError("'%s' field not in '%s' entry" % (field, entry))


def print_status(source, message, newline=False):
    end = "\n" if newline else ""
    print("\033[2K\r" + source + " -- " + message, end=end)

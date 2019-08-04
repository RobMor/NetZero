import abc

import entrypoint


class DataSource(abc.ABC):
    TIME = "time"

    @property
    @abc.abstractmethod
    def summary(self):
        """Returns a string containing a one line summary of this data source."""
        pass

    @abc.abstractmethod
    def __init__(self, config):
        """Sets up the data source's internal state based on the config."""
        pass

    @property
    @abc.abstractmethod
    def columns(self):
        """An iterable containing column names to add to table"""
        pass

    @abc.abstractmethod
    def collect_data(self, start=None, end=None):
        """Collects the data between the start and end date (inclusive)"""
        pass

    @abc.abstractmethod
    def aggregator(self):
        """Returns an dict from column tuples to sqlite3 aggregators"""
        pass

def load():
    for name, entrypoint in entrypoints.get_group_named("netzero.sources"):
        source = entrypoint.load()
        yield name, source
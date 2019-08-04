import abc


class DataSource(abc.ABC):

    @property
    @abc.abstractmethod
    def summary(self):
        """Returns a string containing a one line summary of this data source."""
        pass

    @abc.abstractmethod
    def __init__(self, config):
        """Sets up the data source's internal state based on the config."""
        pass

    def validate_config(self, config, entry, fields):
        if entry not in config:
            raise ValueError("'%s' entry not in config" % entry)

        for field in fields:
            if field not in config[entry]:
                raise ValueError("'%s' field not in '%s' entry" %
                                 (field, entry))

    @abc.abstractmethod
    def collect_data(self, start=None, end=None):
        """Collects the data between the start and end date (inclusive)"""
        pass

    @abc.abstractmethod
    def aggregator(self):
        """Returns an sqlite3 aggregator for this data source"""
        pass

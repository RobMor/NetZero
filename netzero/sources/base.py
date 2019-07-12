import abc


class DataSource(abc.ABC):
    
    def validate_config(self, config, entry, fields):
        if entry not in config:
            raise ValueError("'%s' entry not in config" % entry)

        for field in fields:
            if field not in config[entry]:
                raise ValueError("'%s' field not in '%s' entry" % (field, entry))

    @abc.abstractmethod
    def collect_data(self, start=None, end=None):
        pass

    @abc.abstractmethod
    def process_data(self):
        pass
    
    @property
    @abc.abstractproperty
    def default_start(self):
        pass

    @property
    @abc.abstractproperty
    def default_end(self):
        pass
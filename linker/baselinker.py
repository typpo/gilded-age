import abc

class BaseLinker():
    """Base linking class"""
    __metaclass__ = abc.ABCMeta

    def __init__(self, conn):
        """Initializes with a database connection"""
        self.conn = conn
        self.cur = conn.cursor()

    @abc.abstractmethod
    def resolve(self, item, test=False):
        """Runs linking/resolution on a list of entities."""
        return

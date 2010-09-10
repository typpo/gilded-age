import abc

class BaseAnalyzer():
    """Base analysis class"""

    __metaclass__ = abc.ABCMeta

    def __init__(self, conn):
        """Initializes with a database connection"""
        self.conn = conn
        self.cur = conn.cursor()

    @abc.abstractmethod
    def execute(self, articles):
        """Runs analysis on a list of documents."""
        return

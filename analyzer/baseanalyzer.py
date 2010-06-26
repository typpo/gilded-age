import abc

class BaseAnalyzer(object):
        __metaclass__ = abc.ABCMeta

	def __init__(self, conn):
                """Initializes with a database connection"""
                self.conn = conn
                self.cur = conn.cursor()

	def execute(self, constraints):
		"""Passes documents that match a given SQL constraint:
		date, paper, page, etc.
		"""
		pass

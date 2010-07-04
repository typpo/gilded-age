from baseanalyzer import BaseAnalyzer
from .lib.calais import Calais

class CalaisAnalyzer(BaseAnalyzer):
	"""Class for sending database articles to OpenCalais
	and recording the results."""	
        def __init__(self, conn):
		"""Initialize with database connection"""
                super(CalaisAnalyzer, self).__init__(conn)
                print 'Initializing CalaisAnalyzer...'

	def execute(self, documents):
		print 'Running OpenCalais analysis...'
		for document in documents:
			pass

from baseanalyzer import BaseAnalyzer
from .lib.calais import Calais
from ConfigParser import ConfigParser

CONFIG_PATH = 'analyzer/calais.cfg'

class CalaisAnalyzer(BaseAnalyzer):
	"""Class for sending database articles to OpenCalais
	and recording the results."""	
        def __init__(self, conn):
		"""Initialize with database connection"""
                super(CalaisAnalyzer, self).__init__(conn)
                print 'Initializing CalaisAnalyzer...'
		self.loadkey()
		self.calais = Calais(self.key, submitter='gildedage')

	def loadkey(self):
                config = ConfigParser()
                config.readfp(open(CONFIG_PATH))
		self.key = config.get('config', 'key')

	def execute(self, documents):
		print 'Running OpenCalais analysis...'
		for doc in documents:
			title = doc[4]
			print
			print 'ARTICLE:', title
			article = doc[6]
			if not article is None:
				result = self.calais.analyze(article)
				result.print_topics()
				result.print_relations()
				result.print_entities()

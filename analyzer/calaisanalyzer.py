from .lib.calais import Calais
from baseanalyzer import BaseAnalyzer
from ConfigParser import ConfigParser
import time

# Settings for calais analyzer
CONFIG_PATH = 'analyzer/calais.cfg'

# Table to write calais entities
CALAIS_TABLE = 'calais_items'

# Table to write article-entity relationships
RELATIONSHIP_TABLE = 'calais_results'

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
		"""Loads OpenCalais key from settings file"""
                config = ConfigParser()
                config.readfp(open(CONFIG_PATH))
		self.key = config.get('config', 'key')

	def execute(self, documents):
		"""Runs analysis on documents"""
		print 'Running OpenCalais analysis...'
		for doc in documents:
			id = doc[0]
			title = doc[4]
			print 'ARTICLE %d: %s' % (id, title)
			# Grab full text and send to calais
			article = doc[6]
			if article is not None:
				# Retry if request doesn't go through
				repeat = True
				while repeat:
					try:
						result = self.calais.analyze(article)
						repeat = False
					except ValueError as (errno, strerr):
						print 'ValueError %d: %s' % (errno, strerr)
						print 'Repeating...'
						time.sleep(.25)

				if hasattr(result, 'topics'):
					for topic in result.topics:
						category = topic['categoryName']
						score = topic['score'] if hasattr(topic, 'score') else 0
						type = '_category'

						# Add to db
						self._addLink(id, type, category, score)

				if hasattr(result, 'entities'):
					for entity in result.entities:
						type = entity['_type']
						name = entity['name']
						score = entity['relevance']

						# Add to db
						self._addLink(id, type, name, score)

				if hasattr(result, 'relations'):
					for relation in result.relations:
						type = relation['_type']
						for k,v in relation.items():
							if k.startswith('_'):
								continue
							if isinstance(v, unicode):
								relates_to = v
							elif isinstance(v, dict) and v.has_key('name'):
								relates_to = v['name']
							else:
								continue

							subject = k
							type = '_relation'
							text = '%s _:_ %s' % (subject, relates_to)
							score = 0
							self._addLink(id, type, text, score)

				# limit 4 per second
				time.sleep(.25)

	def _addLink(self, article_id, type, text, score):
		"""Adds this entry to the database and links the article"""
		cur = self.conn.cursor()

		# Check if the database already contains this entry.
		query = 'SELECT * from calais_items WHERE data=? AND type=?'
		cur.execute(query, (text, type))
		result = cur.fetchall()
		n = len(result)
		if n==1:
			# Already exists, so update count.
			count = result[0][3] + 1
			query = 'UPDATE calais_items SET count=? WHERE data=? AND type=?'
			cur.execute(query, (count, text, type))
			# Set id of item we modified.
			relation_id = result[0][0]
		elif n<1:
			# Make a new entry.
			query = 'INSERT INTO calais_items VALUES (null, ?, ?, ?)'
			cur.execute(query, (type, text, 1))
			cur.execute('select last_insert_rowid()')
			# Set id of item we created.
			relation_id = cur.fetchall()[0][0]
		else:
			print 'Duplicate; sonething went wrong when classifying %s, %s for %d' \
				% (type, text, article_id)
			return False

		# Create entry for article id
		query = 'INSERT INTO calais_results VALUES (null, ?, ?, ?, ?)'
		# Assume that the article has full text for now.
		cur.execute(query, (article_id, relation_id, score, True))

		# Send changes.
		self.conn.commit()

		return True

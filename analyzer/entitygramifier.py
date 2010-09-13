from baseanalyzer import BaseAnalyzer
from entitygram import EntityGram

class EntityGramifier(BaseAnalyzer):
    """Provides tools to create entitygrams from a list of articles and
    use that data in meaningful ways."""
    def __init__(self, conn):
        super(EntityGrams, self).__init__(conn)

    def execute(self, articles):
        map(self._fromarticle, articles)

    def _fromarticle(self, article):
        e = EntityGram(article)

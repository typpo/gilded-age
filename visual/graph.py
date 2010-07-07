from .articles import Article

class Graph:
    """Reads database and creates a graph object"""
    def __init__(self, conn):
        self.conn = conn

    def _getrelatedarticles(self, article):
        """Returns articles related by category"""
        if not isinstance(article, Article):
            print 'Wrong type, can\'t build articles graph'
            return

        cur = self.conn.cursor()
        for analyzer in constants.ENABLED_ANALYZERS:
            if analyzer == 'CALAIS':
                query = 'SELECT * from calais_items WHERE article_id=?'
                args = (doc[0])
                cur.execute(query, args)

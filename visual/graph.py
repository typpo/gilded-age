import constants
import db.articles as articles
import db.calaisitems as calaisitems
import db.calaisresults as calaisresults

class Graph:
    """Reads database and creates a graph object"""
    def __init__(self, conn):
        self.conn = conn

    def getAnalysis(self, article):
        """Given an article, return analysis associated with it"""
        if not isinstance(article, articles.Article):
            print 'Wrong type, can\'t build articles graph'
            return

        cur = self.conn.cursor()
        
        ret = []
        for analyzer in constants.ENABLED_ANALYZERS:
            if analyzer == 'CALAIS':
                # Find results linked to this article.
                query = 'SELECT * from calais_results WHERE article_id=?'
                cur.execute(query, (article.id,))
                results = calaisresults.processAll(cur.fetchall())

                for result in results:               
                    # Get the relations linked to the article.
                    query = 'SELECT * from calais_items WHERE id=?'
                    cur.execute(query, (result.relation_id,))
                    relations = calaisitems.processAll(cur.fetchall())

                    ret.extend(relations)
        return ret

    def getRelatedArticles(self, article):
        """Get articles related to a given article"""
        if not isinstance(article, articles.Article):
            print 'Wrong type, can\'t build articles graph'
            return

        cur = self.conn.cursor()
        relations = self.getAnalysis(article)
        
        ret = []
        for analyzer in constants.ENABLED_ANALYZERS:
            if analyzer == 'CALAIS':
                ret = []
                for relation in relations:
                    # Find results concerning the same relation and order by high scores.
                    # TODO needs to be improved
                    query = 'SELECT * from calais_results WHERE relation_id=? order by relevance'
                    cur.execute(query, (relation.id,))
                    results = calaisresults.processAll(cur.fetchall())

                    # Get all the articles associated with these results.
                    for result in results:
                        # TODO avoid duplicates
                        query = 'SELECT * from articles WHERE id=?'
                        cur.execute(query, (result.article_id,))
                        related = articles.processAll(cur.fetchall())

                        ret.extend(related)
                return ret

    def getCategory(self, article):
        """Given an article, return its category"""
        if not isinstance(article, articles.Article):
            print 'Wrong type, can\'t build articles graph'
            return

        cur = self.conn.cursor()
        
        ret = []
        for analyzer in constants.ENABLED_ANALYZERS:
            if analyzer == 'CALAIS':
                # Find results linked to this article.
                query = 'SELECT * from calais_results WHERE article_id=?'
                cur.execute(query, (article.id,))
                results = calaisresults.processAll(cur.fetchall())

                for result in results:               
                    # Get the relations linked to the article.
                    query = 'SELECT * from calais_items WHERE id=? AND type=?'
                    cur.execute(query, (result.relation_id, '_category'))
                    relations = calaisitems.processAll(cur.fetchall())

                    ret.extend(relations)
        return ret

    def getEntities(self, article):
        """Given an article, return its entities"""
        if not isinstance(article, articles.Article):
            print 'Wrong type, can\'t build articles graph'
            return

        cur = self.conn.cursor()
        
        ret = []
        for analyzer in constants.ENABLED_ANALYZERS:
            if analyzer == 'CALAIS':
                # Find results linked to this article.
                query = 'SELECT * from calais_results WHERE article_id=?'
                cur.execute(query, (article.id,))
                results = calaisresults.processAll(cur.fetchall())

                for result in results:               
                    # Get the entities linked to the article.
                    # Anything without a specially reserved type (category, relation) is an entity.
                    query = 'SELECT * from calais_items WHERE id=? AND type!=? AND type!=?'
                    cur.execute(query, (result.relation_id, '_category', '_relation'))
                    entities = calaisitems.processAll(cur.fetchall())

                    ret.extend(entities)
        return ret

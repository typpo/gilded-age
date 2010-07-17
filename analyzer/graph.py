from node import Node
import constants
import db.articles
import db.calaisitems as calaisitems
import db.calaisresults as calaisresults

class Graph:
    """Reads database and creates a graph object, and other basic analytics
    and canned queries."""

    def __init__(self, conn):
        self.conn = conn

    def generateGraph(self, articles):
        """Takes a list of articles, creates and returns a graph object"""
        V = []
        E = []
        G = (V, E)

        for article in articles:
            # Get analysis
            entities = self.getEntities(article)
            category = self.getCategories(article)[0]

            articlenode = Node(article)
            articlenode.category = category
            V.add(articlenode)

            for entity in entities:
                entitynode = Node(entity)
                entitynode.category = category
                V.add(entitynode)
                E.append((articlenode, entitynode))

    def getArticles(self, id=None, source=None, alignment=None, page=None, title=None, summary=None, \
        text=None, url=None, date=None, relevance=None, type=None, type_data=None):
        """Get articles based on a number of article and relationship parameters
        TODO configurable comparison
        TODO make it possible to skip articles query"""
        cur = self.conn.cursor()

        # Prepare articles query
        queryparts = []
        queryargs = []
        if id is not None:
            queryparts.append('id=?')
            queryargs.append(id)
        if source is not None:
            queryparts.append('source=?')
            queryargs.append(source)
        if alignment is not None:
            queryparts.append('alignment=?')
            queryargs.append(alignment)
        if page is not None:
            queryparts.append('page=?')
            queryargs.append(page)
        if title is not None:
            queryparts.append('title=?')
            queryargs.append(title)
        if summary is not None:
            queryparts.append('summary=?')
            queryargs.append(summary)
        if text is not None:
            if text == 'True':
                queryparts.append('text!="None"')
            else:
                queryparts.append('text=?')
                queryargs.append(text)
        if url is not None:
            queryparts.append('url=?')
            queryargs.append(url)
        if date is not None:
            queryparts.append('date=?')
            queryargs.append(url)

        if len(queryparts) < 1:
            return self.getAnalysis(type=type, type_data=type_data, relevance=relevance)
        else:
            query = 'SELECT * FROM articles'
            if len(queryparts) > 0:
                query += ' WHERE '
            query += ' AND '.join(queryparts)

            # Execute articles query
            cur.execute(query, tuple(queryargs))
            articles = db.articles.processAll(cur.fetchall())

            # See if we need to get analysis
            if relevance is None and type is None and type_data is None:
                return articles

            # We need to apply analysis parameters
            ret = []
            for article in articles:
                # Get article analysis
                ret.extend(self.getAnalysis(article_id=article.id, type=type, type_data=type_data, relevance=relevance))
            return ret

    def getAnalysis(self, article_id=None, type=None, type_data=None, relevance=None):
        """Given an article, return analysis associated with it"""
        cur = self.conn.cursor()
        
        ret = []
        for analyzer in constants.ENABLED_ANALYZERS:
            if analyzer == 'CALAIS':
                # Find results linked
                queryparts = []
                queryargs = []

                if relevance is not None:
                    queryparts.append('relevance=?')
                    queryargs.append(relevance)
                if article_id is not None:
                    # Look only for supplied article
                    queryparts.append('article_id=?')
                    queryargs.append(article.id)

                # Build query
                query = 'SELECT * from calais_results'
                if len(queryparts) > 0:
                    query += ' WHERE '
                query += ' AND '.join(queryparts)

                # Execute it
                cur.execute(query, tuple(queryargs))
                results = calaisresults.processAll(cur.fetchall())

                for result in results:               
                    # Get the relations linked to the article.
                    queryparts = []
                    queryargs = []
                    if type is not None:
                        queryparts.append('type=?')
                        queryargs.append(type)
                    if type_data is not None:
                        queryparts.append('data=?')
                        queryargs.append(type_data)

                    # set ID
                    queryparts.append('id=?')
                    queryargs.append(result.relation_id)

                    # Build query
                    query = 'SELECT * from calais_items WHERE '
                    query += ' AND '.join(queryparts)

                    # Execute query
                    cur.execute(query, tuple(queryargs))
                    relations = calaisitems.processAll(cur.fetchall())

                    ret.extend(relations)
        return ret

    def getRelatedArticles(self, article):
        """Get articles related to a given article"""
        if not isinstance(article, db.articles.Article):
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
                        related = db.articles.processAll(cur.fetchall())

                        ret.extend(related)
                return ret

    def getCategories(self, article):
        """Given an article, return its category"""
        if not isinstance(article, db.articles.Article):
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
        if not isinstance(article, db.articles.Article):
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

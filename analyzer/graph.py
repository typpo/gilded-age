from node import Node
import constants
import db.articles
import db.calaisitems
import db.calaisresults

# SQL comparators to accept in api calls
VALID_SQL_COMPARATORS = ['=', '!=', '<', '<=', '>', '>=', 'LIKE']

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
        text=None, url=None, date=None, relevance=None, result_type=None, result_data=None):
        """Get articles based on a number of article and relationship parameters.

        Except for noted below, all parameters are tested for exact equality:
        title, summary, text -- specifies data is LIKE

        TODO pass in dict instead of params. Dict keys should be validated"""

        cur = self.conn.cursor()

        # Prepare articles query
        queryparts = []
        queryargs = ()
        if id is not None:
            clause, args = self._buildClause('id', id)
            queryparts.append(clause)
            queryargs += args
        if source is not None:
            clause, args = self._buildClause('source', source)
            queryparts.append(clause)
            queryargs += args
        if alignment is not None:
            clause, args = self._buildClause('alignment', alignment)
            queryparts.append(clause)
            queryargs += args
        if page is not None:
            clause, args = self._buildClause('page', page)
            queryparts.append(clause)
            queryargs += args
        if title is not None:
            clause, args = self._buildClause('title', title, \
                comparator='LIKE')
            queryparts.append(clause)
            queryargs += args
        if summary is not None:
            clause, args = self._buildClause('summary', summary, \
                comparator='LIKE')
            queryparts.append(clause)
            queryargs += args
        if text is not None:
            if type(text)==bool:
                if text:
                    # Ensure that there is text in this article.
                    clause, args = self._buildClause('text', 'None', \
                        comparator='!=')
                else:
                    # No text
                    clause, args = self._buildClause('text', 'None', \
                        comparator='==')
            else:
                clause, args = self._buildClause('text', text, \
                    comparator='LIKE')
            queryparts.append(clause)
            queryargs += args
        if url is not None:
            clause, args = self._buildClause('url', url)
            queryparts.append(clause)
            queryargs += args
        if date is not None:
            clause, args = self._buildClause('date', date)
            queryparts.append(clause)
            queryargs += args
            
        if relevance is None and result_type is None and result_data is None:
            # Don't need to join tables because we're just looking at articles.
            query = 'SELECT * FROM articles'
            if len(queryparts) > 0:
                query += ' WHERE '
                query += ' AND '.join(queryparts)
            
            # Execute articles only query
            cur.execute(query, queryargs)
            articles = db.articles.processAll(cur.fetchall())
        else:
            # Need to join tables because we're looking at analysis results as well
            query = 'SELECT * FROM articles INNER JOIN calais_results ON articles.id=calais_results.article_id INNER JOIN calais_items ON calais_results.relation_id=calais_items.id WHERE '
            # TODO add query parts - could be further optimized if relevance is separate from type, result_data
            if result_type is not None:
                clause, args = self._buildClause('type', result_type)
                queryparts.append(clause)
                queryargs += args
            if result_data is not None:
                clause, args = self._buildClause('data', result_data, \
                    comparator='LIKE')
                queryparts.append(clause)
                queryargs += args
            if relevance is not None:
                clause, args = self._buildClause('relevance', relevance, \
                    comparator='>=')
                queryparts.append(clause)
                queryargs += args

            # Build query
            query += ' AND '.join(queryparts)

            # Execute query on table join
            cur.execute(query, queryargs)
            articles = db.articles.processAll(cur.fetchall())

            # TODO group by article id, because we throw away other article result metadata (which causes duplication of articles)

        return articles

    def getArticlesWithRelationship(self, article_id=None, result_type=None, result_data=None,\
        relevance=None):
        """Find articles that match certain parameters"""

        cur = self.conn.cursor()
        
        ret = []
        for analyzer in constants.ENABLED_ANALYZERS:
            if analyzer == 'CALAIS':
                # Get the relations linked to the article.
                queryparts = []
                queryargs = ()
                if result_type is not None:
                    clause, args = self._buildClause('type', result_type)
                    queryparts.append(clause)
                    queryargs += args
                if result_data is not None:
                    clause, args = self._buildClause('data', result_data, \
                        comparator='LIKE')
                    queryparts.append(clause)
                    queryargs += args

                # Build query to filter by parameters
                query = 'SELECT * from calais_items WHERE '
                query += ' AND '.join(queryparts)

                # Execute query
                cur.execute(query, queryargs)
                calaisitems = db.calaisitems.processAll(cur.fetchall())

                # TODO quit if there aren't enough results

                # Build a query to get results that match
                relationids = [item.id for item in calaisitems]
                queryparts = []
                queryargs = ()

                # TODO query building fails if multiple constraints are provided

                # filter relevance
                if relevance is not None:
                    clause, args = self._buildClause('relevance', relevance, \
                        comparator='>=')
                    queryparts.append(clause)
                    queryargs += args

                # filter article id
                if article_id is not None:
                    clause, args = self._buildClause('article_id', article_id)
                    queryparts.append(clause)
                    queryargs += args

                # Filter by relationships gotten from the last query
                # We assume relationids contains at least one item
                clause, args = self._buildClause('relation_id', relationids)
                queryparts.append(clause)
                queryargs += args

                query = 'SELECT * FROM calais_results WHERE '
                query += ' AND '.join(queryparts)

                cur.execute(query, queryargs)
                calaisresults = db.calaisresults.processAll(cur.fetchall())

                # Now return articles for each result
                query = 'SELECT * FROM articles WHERE '
                articleids = [result.article_id for result in calaisresults]
                # TODO we assume articleids contains > 0 items
                clause, args = self._buildClause('id', articleids)
                query += clause

                cur.execute(query, args)
                ret.extend(cur.fetchall())

        return db.articles.processAll(ret)

    def getAnalysis(self, article_id=None, result_type=None, result_data=None, relevance=None):
        """Given an article, return analysis associated with it.

        Except for noted below, all parameters are tested for exact equality:
        relevance -- specifies score of at least X
        result_data -- specifies data is LIKE
        """

        cur = self.conn.cursor()
        
        ret = []
        for analyzer in constants.ENABLED_ANALYZERS:
            if analyzer == 'CALAIS':
                # Find results linked
                queryparts = []
                queryargs = ()

                if relevance is not None:
                    clause, args = self._buildClause('relevance', relevance, \
                        comparator='>=')
                    queryparts.add(clause)
                    queryargs += args
                if article_id is not None:
                    # Limits results to those pertaining to given article ids
                    clause, args = self._buildClause('article_id', article_id)
                    queryparts.add(clause)
                    queryargs += args

                # Build query
                query = 'SELECT * from calais_results'
                if len(queryparts) > 0:
                    query += ' WHERE '
                query += ' AND '.join(queryparts)

                # Execute it
                cur.execute(query, tuple(queryargs))
                results = db.calaisresults.processAll(cur.fetchall())

                for result in results:               
                    # Get the relations linked to the article.
                    queryparts = []
                    queryargs = []
                    if result_type is not None:
                        queryparts.append('type=?')
                        queryargs.append(result_type)
                    if result_data is not None:
                        queryparts.append('data LIKE ?')
                        queryargs.append(result_data)

                    # set id
                    queryparts.append('id=?')
                    queryargs.append(result.relation_id)

                    # Build query
                    query = 'SELECT * from calais_items WHERE '
                    query += ' AND '.join(queryparts)

                    # Execute query
                    cur.execute(query, tuple(queryargs))
                    relations = db.calaisitems.processAll(cur.fetchall())

                    ret.extend(relations)
        return ret

    def _buildClause(self, field, values, comparator='='):
        """OR's together values of a field to construct a sql where clause

        returns -- (clause of sql query, parameter-bound arguments)
        """
        if comparator not in VALID_SQL_COMPARATORS:
            print 'Bad comparator "%s": not allowed.' % (comparator)
            return None, None

        if type(values) is list:
            items = ['%s %s ?' % (field, comparator)]*len(values)
            clause = '(%s)' % (' OR '.join(items))
            args = tuple(values)
        else:
            clause = '%s %s ?' % (field, comparator)
            args = (values,)

        return clause, args

    #
    # --- OLDER FUNCTIONS that work on single articles. ---
    #

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
                    results = db.calaisresults.processAll(cur.fetchall())

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
                results = db.calaisresults.processAll(cur.fetchall())

                for result in results:               
                    # Get the relations linked to the article.
                    query = 'SELECT * from calais_items WHERE id=? AND type=?'
                    cur.execute(query, (result.relation_id, '_category'))
                    relations = db.calaisitems.processAll(cur.fetchall())

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
                results = db.calaisresults.processAll(cur.fetchall())

                for result in results:               
                    # Get the entities linked to the article.
                    # Anything without a specially reserved type (category, relation) is an entity.
                    query = 'SELECT * from calais_items WHERE id=? AND type!=? AND type!=?'
                    cur.execute(query, (result.relation_id, '_category', '_relation'))
                    entities = db.calaisitems.processAll(cur.fetchall())

                    ret.extend(entities)
        return ret

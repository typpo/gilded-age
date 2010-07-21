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

    def getArticles(self, id=None, source=None, alignment=None, page=None, title=None, \
        summary=None, text=None, url=None, date=None, relevance=None, result_type=None, \
        result_data=None, graph=False, limit=None):
        """Get articles based on a number of article and relationship parameters.

        Except for noted below, all parameters are tested for exact equality:
        title, summary, text -- specifies data is LIKE

        TODO specify date comparators
        """

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
            if limit is not None:
                query += ' LIMIT ' + int(limit)
            
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

            # TODO Order by causes problems now that the graph option can skip here 
            # without adding query parts.
            query += ' ORDER BY relevance'
            if limit is not None:
                query += ' LIMIT ' + str(int(limit))

            # Execute query on table join
            cur.execute(query, queryargs)
            results = cur.fetchall()
            articles = db.articles.processAll(results)

            if graph:
                self.buildGraph(results)

        return articles

    def buildGraph(self, results):
        """Creates a graph of results.
        results -- the rows of a join between the three db tables
        """
        # Output a graph of relationships
        print 'Building graph...'
        southnodes = []
        northnodes = []
        linknodes = []
        edges = []
        labels= {}
        for result in results:
            articletitle = result[4]
            link = result[17]
            side = result[2]
            if side=='north':
                northnodes.append(articletitle)
            else:
                southnodes.append(articletitle)
            linknodes.append(link)
            labels[link] = link
            labels[articletitle] = ''
            edges.append((articletitle,link))

        # Import graphing libraries
        import matplotlib
        # Switch default output from X11
        matplotlib.use('Agg')

        import networkx as nx
        import matplotlib.pyplot as plt

        # Draw this graph
        print 'Drawing figure... %d results' % len(results)
        G = nx.Graph()
        G.add_nodes_from(southnodes)
        G.add_nodes_from(northnodes)
        G.add_nodes_from(linknodes)
        G.add_edges_from(edges)

        print 'Computing layout...'
        pos=nx.spring_layout(G)

        print 'north...'
        nx.draw_networkx_nodes(G, pos, nodelist=northnodes, node_color='blue', node_size=90, alpha=.5)
        print 'south...'
        nx.draw_networkx_nodes(G, pos, nodelist=southnodes, node_color='red', node_size=90, alpha=.5)
        print 'links...'
        nx.draw_networkx_nodes(G, pos, nodelist=linknodes, node_color='green', node_size=90, alpha=.5)
        print 'edges...'
        nx.draw_networkx_edges(G, pos, edgelist=edges)
        print 'labels...'
        nx.draw_networkx_labels(G, pos, labels, font_size=8, font_color='#ff6600')
        print 'Saving figure...'
        plt.axis('tight')
        plt.savefig('outputgraph.png')

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
                    # Find results for the same relation and order by high scores.
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

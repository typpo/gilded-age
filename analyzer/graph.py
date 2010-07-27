from node import Node
from ConfigParser import ConfigParser
import constants
import db.articles
import db.calaisitems
import db.calaisresults

# Import graphing libraries
import matplotlib
# Switch default output from X11
matplotlib.use('Agg')

import networkx as nx
import matplotlib.pyplot as plt

# SQL comparators to accept in api calls.
VALID_SQL_COMPARATORS = ['=', '!=', '<', '<=', '>', '>=', 'LIKE']

# Load graph construction/analysis settings from configuration file.
config = ConfigParser()
config.readfp(open(constants.GRAPH_CONFIG))
VALID_SQL_COLUMNS  = config.get('database', 'cols').split(',')
BASE_QUERY = config.get('database', 'base')

class Graph:
    """Reads database and creates a graph object, and other basic analytics
    and canned queries."""

    def __init__(self, conn):
        self.conn = conn

    def graph(self, n, **kwargs):
        cur = self.conn.cursor()
        query, queryargs = self._buildQueryFromArgs(None, limit=n, **kwargs)
        cur.execute(query, queryargs)
        results = cur.fetchall()

        totalresults = len(results)
        print totalresults, 'query results'

        # Get dictionaries of article-concept relationships
        articles, concepts = self._articleConceptRelations(results)

        # Now link related concepts together
        conceptedges = self._linkRelatedConcepts(articles, concepts)

        # Put this on a graph and write it to file
        print 'Done'

    def _articleConceptRelations(self, results):
        """Record relationships between articles and concepts, concepts and articles
        using two dictionaries."""

        concepts = {}
        articles = {}
        for result in results:
            article = result[0]
            concept = result[17]

            if concept not in concepts:
                concepts[concept] = []
            if article not in articles:
                articles[article] = []

            # Add article under concept
            concepts[concept].append(article)

            # Add concept under article
            articles[article].append(concept)

        return articles, concepts

    def _linkRelatedConcepts(self, articles, concepts):
        """Creates edges connecting concepts that are 'related,' where related
        means that they share a common article.
        """

        # Dictionary keyed by edges between concepts, value is the number of 
        # times those edges occur.
        conceptedges = {}
        for concept in concepts:
            # Find concepts associated with this one

            # Search through articles associated with this concept
            for article in concepts[concept]:

                # Go through concepts associated with each article
                for otherconcept in articles[article]:
                    # Skip self-reference
                    if concept == otherconcept:
                        continue

                    # Potential new edges
                    edge = (concept, otherconcept)
                    edge2 = (otherconcept, concept)

                    # Test for duplicates
                    if edge2 in conceptedges:
                        continue

                    if edge not in conceptedges:
                        conceptedges[edge] = 0
                    conceptedges[edge] += 1
        return conceptedges

    def _createGraph(self, concepts, conceptedges):
        g = nx.Graph()
        g.add_nodes_from(concepts.keys())

        # Prune by # of edges
        added_edges = []
        for edge in conceptedges:
            # TODO make this dynamic - maybe based on average degree
            if conceptedges[edge] > 10:
                g.add_edge(edge[0], edge[1])
                added_edges.append(edge)

        print 'Computing layout...', len(added_edges), 'edges and', len(concepts.keys()), 'nodes'
        pos = nx.graphviz_layout(g, prog='twopi')
        nx.draw_networkx_nodes(g, pos, nodelist=concepts.keys(), node_color='white',\
            node_size=80, alpha=.2)
        nx.draw_networkx_edges(g, pos, edgelist=added_edges)
        labels = dict([(x, x) for x in concepts.keys()])
        nx.draw_networkx_labels(g, pos, labels, font_size=8, font_color='green')

        plt.axis('tight')
        plt.savefig('outputgraph.png')

    def getAnalysis(self, limit, **kwargs):
        """Get articles based on a number of article and relationship parameters.

        To specify a comparator for a given column, set '_columnname', where 
        columnname is the name of the column.

        limit -- number of distinct results
        """

        cur = self.conn.cursor()

        # First, get all distinct results to find the total number of concepts.
        # Use the 'data' field (the result of analysis) to determine unique
        # results.
        query, queryargs = self._buildQueryFromArgs(limit=limit, **kwargs)
        cur.execute(query, queryargs)
        results = cur.fetchall()
        return db.articles.processAll(results)

    def _getRelatedAnalysis(self, articlerestults):
        """Gets entities of all articles"""

        total = len(articleresults)
        print 'Grabbing analysis for %d results...' % total
        for article in articleresults:
            i = articleresults.index(article)
            percent = (float(i) / float(total))*100
            print '%d%%' % percent
            self.getEntities(article)

    def _buildQueryFromArgs(self, **kwargs):
        """Builds an SQL query from named parameters.
        This can handle any fields in all 3 (current) analysis tables as 
        named arguments, as specified in cfg/graph.cfg.

        Special fields that correspond to SQL query options:
            limit - to limit number of results

        To specify a comparator for an argument, set the argument's name
        prepended with a "_", eg. "_data='LIKE'"
        
        Default comparator is equals (=)
        """

        # TODO build date expression

        # Build queries for articles table:
        queryparts = []
        queryargs = ()
        for arg in kwargs:
            # Special fields
            if arg=='limit' or (len(arg) > 0 and arg[0]=='_'):
                continue

            # Validate fields
            if arg not in VALID_SQL_COLUMNS:
                print 'Invalid column will not be included in query:', arg
                continue

            # See if a specific comparator was supplied
            comparator_key = '_' + arg
            override_comparator = '=' 
            if comparator_key in kwargs:
                override_comparator = kwargs[comparator_key]
                if override_comparator not in VALID_SQL_COMPARATORS:
                    print 'Invalid comparator %s for column %s' % \
                        (override_comparator, arg)
                    return None, None

            clause, qargs = self._buildClause(arg, kwargs[arg],\
                comparator=override_comparator)
            queryparts.append(clause)
            queryargs += qargs

        # Build query
        query = BASE_QUERY
        if len(queryparts) > 0:
            query += ' WHERE '
            query += ' AND '.join(queryparts)

        # Order by # of occurences in overall analysis
        query += ' ORDER BY count DESC,relevance DESC'

        if 'limit' in kwargs:
            query += ' LIMIT ' + str(int(kwargs['limit']))

        return query, queryargs

    def _buildClause(self, field, values, comparator='='):
        """ORs together values of a field to construct a sql where clause

        returns -- (clause of sql query, parameter-bound arguments)
        """

        if type(values) is list:
            items = ['%s %s ?' % (field, comparator)]*len(values)
            clause = '(%s)' % (' OR '.join(items))
            args = tuple(values)
        else:
            clause = '%s %s ?' % (field, comparator)
            args = (values,)

        return clause, args

    def buildGraph(self, results):
        """Creates a graph from an array of SQL query results.
        results -- the rows of a join between the three db tables
        """
        # Output a graph of relationships
        print 'Building graph...'

        # Lists containing nodes to be added in graph
        articlenodes = []
        linknodes = []

        # size contains node name and the number of times it appears.
        # This is done to make bigger link nodes appear larger.
        size = {}

        # contains counts of article alignment, used for coloring
        sidecount = {}
        
        # Other setup

        # Edges between articles and concepts
        article_edges = []
        # Labels - keyed by node, with string value for label
        labels= {}

        # Analyze results
        for result in results:
            articleid = result[0]
            link = result[17].lower()
            side = result[2]
            # Set weight of this edge equal to relevance score between 0 and 1.
            # If relevance is 0, then it is from an unweighted field.
            weight = result[13] if result[13] > 0 else 1.0
            articlenodes.append(articleid)

            # Add to graph
            linknodes.append(link)
            edge = (articleid, link)
            article_edges.append(edge)

            # Set labels of nodes
            labels[link] = link

            # Set size of nodes
            size[link] = size[link] + 1 if link in size else 1
            size[articleid] = size[articleid] + 1 if articleid in size else 1

            # Set alignment
            if link not in sidecount:
                sidecount[link] = {'north':0, 'south':0}
            sidecount[link][side] = sidecount[link][side] + 1

        # Draw this graph
        print 'Drawing figure... %d results' % len(results)

        # Clear old graph
        plt.clf()

        # Build networkx graph
        G = nx.Graph()

        # Booleans are strings to workaround bug described here:
        # http://groups.google.com/group/networkx-discuss/browse_thread/thread/dd4f481d63d69c5e
        G.add_nodes_from(linknodes, linkNode='True')
        G.add_edges_from(article_edges)

        # Adjust graph, connecting links and removing articles
        # This reduces clutter and can make the graph more meaningful.
        # TODO make this optional
        # TODO keep track of edges and show them only if they pass a certain threshold
        print 'Linking non-articles...'

        # Dictionary keyed by edges, value indicates how many times they appear.
        linkedges = {}
        for nodedata in G.nodes(data=True):
            if not 'linkNode' in nodedata[1]:
                continue

            node = nodedata[0]

            # Edges between this link node and articles
            edges = G.edge[node]

            if len(edges) > 1:
                # Connect this link node wth the link nodes of all its associated
                # articles.
                for articleid in edges.keys():
                    for otherlink in G.edge[articleid]:
                        if node != otherlink:
                            newedge = (node, otherlink)
                            if newedge not in linkedges:
                                linkedge = (node, otherlink)
                                linkedges[linkedge] = linkedges[linkedge] + 1 \
                                    if linkedge in linkedges else 1

            # remove existing edges
            for connectednode in edges.keys():
                # remove edge
                G.remove_edge(node, connectednode)

        # remove article nodes.  All edges were just removed above.
        G.remove_nodes_from(articlenodes)
        G.remove_edges_from(article_edges)

        # add edges between links, but only if they appear a certain number of times.
        # This is the improve readability and stay within memory limits, because
        # the positioning algorithm is quadratic.
        for edge in linkedges:
            if linkedges[edge] > 10:
                G.add_edge(edge)
        print 'Pruned', len(linkedges), 'edges to', len(G.edges())

        # Compute layout with graphviz
        print 'Computing layout...', len(G.edges()), 'edges and', len(G.nodes()), 'nodes'
        pos = nx.graphviz_layout(G, prog='twopi')

        # Converts rgb values to hex for coloring the nodes
        def rgb_to_hex(rgb):
            return '#%02x%02x%02x' % rgb

        print 'links...'
        for node in linknodes:
            # calculate color
            nsouth = sidecount[node]['south']
            nnorth = sidecount[node]['north']
            total = nsouth + nnorth
            rgb_red = (float(nsouth) / total) * 255
            rgb_blue = (float(nnorth) / total) * 255
            hex = rgb_to_hex((rgb_red, 0, rgb_blue))

            # draw
            nx.draw_networkx_nodes(G, pos, nodelist=[node], node_color=hex,\
                node_size=80+size[node], alpha=.2)
        print 'edges...'
        # TODO something based on edge weight...
        nx.draw_networkx_edges(G, pos, edgelist=linkedges)
        print 'labels...'
        nx.draw_networkx_labels(G, pos, labels, font_size=8, \
            font_color='green')
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
                # TODO Join this with below
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

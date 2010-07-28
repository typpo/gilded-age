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

        # Run query
        print 'Running query'
        query, queryargs = self._buildQueryFromArgs(limit=n, **kwargs)
        cur.execute(query, queryargs)
        results = cur.fetchall()

        totalresults = len(results)
        print totalresults, 'query results'

        # Get dictionaries of article-concept relationships
        print 'Creating hash tables'
        articles, concepts = self._articleConceptRelations(results)

        # Now link related concepts together
        print 'Linking concepts'
        conceptedges = self._linkRelatedConcepts(articles, concepts)

        # Put this on a graph and write it to file
        print 'Writing graph'
        self._writeGraph(concepts, conceptedges)

        print 'Done'

    def _articleConceptRelations(self, results):
        """Record relationships between articles and concepts, concepts and 
        articles using two dictionaries."""

        concepts = {}
        articles = {}
        for result in results:
            article = result[0]
            concept = result[10]
            side = result[2]

            if concept not in concepts:
                # Create basic concept structure
                # Concept
                # |- # articles total
                # |- articles list
                # |- alignment
                # | |- # north
                # | |- # south
                concepts[concept] = {'articles':[], \
                    'alignment':{'north':0,'south':0}}

            if article not in articles:
                # Article structure is just a list of concepts.
                articles[article] = []

            # Add article under concept
            concepts[concept]['articles'].append(article)

            # Keep track of concept alignment
            concepts[concept]['alignment'][side] += 1

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
            for article in concepts[concept]['articles']:

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

    def _writeGraph(self, concepts, conceptedges):
        g = nx.Graph()
        g.add_nodes_from(concepts.keys())

        # Prune by # of edges
        added_edges = []
        for edge in conceptedges:
            # TODO make this dynamic - maybe based on average degree
            if conceptedges[edge] > 200:
                g.add_edge(edge[0], edge[1])
                added_edges.append(edge)

        # Compute node positions
        print 'Computing layout...', len(added_edges), 'edges and', len(concepts.keys()), 'nodes'
        pos = nx.graphviz_layout(g, prog='twopi')

        # Add nodes for each concept
        for concept in concepts:
            # Compute color
            numnorth = concepts[concept]['alignment']['north']
            numsouth = concepts[concept]['alignment']['south']
            total = numnorth + numsouth
            rgb_red = (float(numsouth) / total) * 255
            rgb_blue = (float(numnorth) / total) * 255
            hex_color = '#%02x%02x%02x' % (rgb_red, 0, rgb_blue)

            # Compute size
            size = 80 + total

            # Draw nodes
            nx.draw_networkx_nodes(g, pos, nodelist=[concept], \
                node_color=hex_color, node_size=size, alpha=.8)

        # Draw edges
        nx.draw_networkx_edges(g, pos, edgelist=added_edges)

        # Draw labels
        labels = dict([(x, x) for x in concepts.keys()])
        nx.draw_networkx_labels(g, pos, labels, font_size=8, font_color='green')

        # Draw graph and save
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

    def getAnalysis(self, article_id=None, result_type=None, result_data=None, relevance=None):
        """Given an article, return analysis associated with it.

        Except for noted below, all parameters are tested for exact equality:
        relevance -- specifies score of at least X
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
                        queryparts.append('data=?')
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

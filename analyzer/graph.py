from ConfigParser import ConfigParser
import constants
import db.articles
import db.calaisitems
import db.calaisresults

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

    def graph(self, results=None):
        """Creates a graph representing links between various entities and
        concepts extracted in analysis. Writes an illustration to file.
        """
        if results is None:
            results = self._runQueryFromArgs(n, **kwargs)
        concepts, conceptedges = self._computeConceptGraph(results)

        import matplotlib
        # Switch default output from X11
        matplotlib.use('Agg')

        # Put this on a graph and write it to file.
        print 'Writing graph'
        self._writeGraph(concepts, conceptedges)

        print 'Done'

    def csv(self, results=None):
        """Writes a CSV that indicates relations between concepts and the 
        number of times they occur.  This is different from the number of 
        times words appear together in text."""
            # TODO normalize by number of articles
        if results is None:
            results = self._runQueryFromArgs(n, **kwargs)
        conceptdict, conceptedges = self._computeConceptGraph(results)
        # Concept
        # |- articles list
        # |- alignment
        # | |- # north
        # | |- # south
        concepts = conceptdict.keys()
        concepts.sort()

        import csv
        writer = csv.writer(open('csv_output.csv', 'wb'), delimiter='\t')

        # Construct top row of csv
        top_row = ['label']
        top_row.extend(concepts)

        # Add all the concepts to our graph
        # and initialize the matrix.
        # This is n^2 with a dict but we do a lot of lookups later.
        d = {}
        for concept in concepts:
            d[concept] = {}
            for other_concept in concepts:
                d[concept][other_concept] = 0

        # TODO unfortunately building a matrix means we have to duplicate 
        # values - the edge graph is undirected (ie. does not 
        # create duplicates), so we need to account for this.
        # But let's just see if this works for now.
        for edge in conceptedges:
            a,b = edge
            d[a][b] += conceptedges[edge]
            d[b][a] += conceptedges[edge]

        # Write to file
        print 'Building csv'
        writer.writerow(top_row)

        sources = d.keys()
        sources.sort()
        for source in sources:
            # Construct a row
            row = [source]
            destinations = d[source].keys()
            destinations.sort()
            for dest in destinations:
                # Number of times this link appears
                row.append(d[source][dest])
            writer.writerow(row)

    def plot(self, n, **kwargs):
        """Plots query results over time.
        n -- maximum number of distinct results"""
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import datetime

        # Query for article
        articles = self.getArticles(n, **kwargs)
        print len(articles), 'article hits for query'

        # Build dict keyed by date, with value indicating number of hits
        results = {}
        for article in articles:
            # Parse date
            datestr = article.articleDate
            date = datetime.datetime.strptime(datestr[:datestr.find(' ')], \
                '%Y-%m-%d')
            # round down to first of month
            date = datetime.date(date.year, date.month, 1)
            if date not in results:
                results[date] = 0
            results[date] += 1

        # TODO normalize for total number of articles

        tuples = zip(results.keys(), results.values())
        tuples.sort(lambda a,b: 1 if a[0] > b[0] else -1 if a[0] < b[0] else 0)
        x,y = zip(*tuples)
        plt.plot_date(x, y, linestyle='-')

        plt.xlabel('Date')
        plt.ylabel('Occurrences')
        plt.title(r'Plot')

        plt.grid(True)
        plt.axis('tight')
        plt.savefig('outputgraph.png')

    def getArticles(self, limit, **kwargs):
        """Get articles based on a number of article and relationship parameters.

        To specify a comparator for a given column, set '_columnname', where 
        columnname is the name of the column.

        limit -- maximum number of distinct results
        """
        cur = self.conn.cursor()

        query, queryargs = self._runQueryFromArgs(limit=limit, **kwargs)
        cur.execute(query, queryargs)
        results = cur.fetchall()
        return db.articles.processAll(results)


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

    def _computeConceptGraph(self, results):
        """Creates a graph representing links between various entities and
        concepts extracted in analysis. Returns a list of concepts and the
        edges connecting them.
        """
        # Get dictionaries of article-concept relationships
        print 'Creating hash tables'
        articles, concepts = self._articleConceptRelations(results)

        # Now link related concepts together
        print 'Linking concepts'
        conceptedges = self._linkRelatedConcepts(articles, concepts)
        
        return concepts, conceptedges

    def _writeGraph(self, concepts, conceptedges):
        import networkx as nx
        import matplotlib.pyplot as plt
        import numpy as np
        import matplotlib.mlab as mlab

        g = nx.Graph()
        g.add_nodes_from(concepts.keys())

        # Prune by # of edges
        added_edges = []
        for edge in conceptedges:
            # TODO make this dynamic - maybe based on average degree
            if conceptedges[edge] > 100:
                g.add_edge(edge[0], edge[1])
                added_edges.append(edge)

        # Compute node positions
        print 'Computing layout...', len(added_edges), 'edges and', len(concepts.keys()), 'nodes'
        pos = nx.circular_layout(g)

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

    def _runQueryFromArgs(self, **kwargs):
        """Builds an SQL query from named parameters, or just returns an SQL 
        query if it was specified.

        Also, runs the query.

        Guide to building a query from components:

        This can handle any fields in all 3 (current) analysis tables as 
        named arguments, as specified in cfg/graph.cfg

        Special fields that correspond to SQL query options:
            limit - to limit number of results

        To specify a comparator for an argument, set the argument's name
        prepended with a "_", eg. "_data='LIKE'"
        
        Default comparator is equals (=)
        """
        
        if 'query' in kwargs:
            # Full query string is supplied, so use it.
            query = kwargs['query']
            queryargs = kwargs['queryargs']
        else:
            # Build a query string and incorporate specified
            # components.

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
            query += ' ORDER BY count DESC'

            if 'limit' in kwargs:
                query += ' LIMIT ' + str(int(kwargs['limit']))

        # Run query
        cur = self.conn.cursor()
        print 'Running query'
        query, queryargs = self._runQueryFromArgs(limit=n, **kwargs)
        cur.execute(query, queryargs)
        results = cur.fetchall()

        totalresults = len(results)
        print totalresults, 'query results'
        return results


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

    def glomMetadata(self, articles):
        """Loops through given articles and adds metadata (entity data)
        to the result.  Will duplicate articles with multiple entries.
        
        This is slow for many articles!  But because FTS has problems with
        joining, it is a temporary solution."""

        cur = self.conn.cursor()
        
        ret = []
        print 'Glomming', len(articles), 'articles...'
        for article in articles:
            # TODO omit timeEnter?
            articledata = (article.id, article.source, article.alignment, \
                article.page, article.title, article.summary, article.text, \
                article.url, article.articleDate, article.timeEnter)

            # Find results linked to this article.
            query = 'SELECT * from calais_results WHERE article_id=?'
            cur.execute(query, (article.id,))
            results = db.calaisresults.processAll(cur.fetchall())

            # TODO join these

            for result in results:               
                # Get all relations linked to the article.
                query = 'SELECT * from calais_items WHERE id=?'
                cur.execute(query, (result.relation_id,))
                relations = db.calaisitems.processAll(cur.fetchall())

                for relation in relations:
                    fulldata = articledata + (relation.type, relation.data, \
                        relation.count)
                    ret.append(fulldata)

        print 'Done'
        return ret

    #
    # --- OLDER FUNCTIONS that work on single articles. Also, their queries 
    # need to be rewritten to joins...unless FTS won't work that way?---
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

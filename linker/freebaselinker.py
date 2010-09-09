import freebase
import operator

# Freebase relevance threshold to consider an entity for inclusion  in a
# semantic group.  This is a value (roughly a percentage) that indicates 
# certainty.
GROUPING_THRESHOLD = 75.0

# Some mappings that we already know about, for more accurate search results
CALAIS_TYPE_MAPPING = {
    'Person':'/people/person',
    'City':'/location/citytown',
    'Company':'/business/company',
    'Continent':'/location/continent',
    'Currency':'/finance/currency',
    'Holiday':'/time/holiday',
    'MedicalCondition':'/medicine/disease',
    'NaturalFeature':'/geography/geographical_feature',
    'ProvinceOrState':'/location/location',
    'Region':'/location/location'
}
# Not added:
#    'Facility',
# MedicalTreatment
#    'Organization':'/organization/organization',

class FreebaseLinker(BaseLinker):
    """Resolves ambiguous entities using Freebase
    Set attribute fts to use full text search (FTS) database or not.
    """

    # Indicates if FTS operators can be used on the database, which is much 
    # faster.
    fts = True

    def __init__(self, conn):
        print 'Initializing FreebaseLinker...'
        super(FreebaseLinker, self).__init__(conn)

    def test(self, q=10):
        """Tries to resolve a few entities
        q -- number of entities to disambiguate"""
        cur = self.conn.cursor()

        # Get a few items to disambiguate
        query = 'select * from calais_items where type=? order by count desc limit ?'
        cur.execute(query, ('Person', q))
        r = cur.fetchall()

        # Resolve them
        import db.calaisitems
        c = db.calaisitems.processAll(r)
        self.resolveAll(c, test=True)

    def resolveAll(self, itemlist, test=False):
        """Takes a list of calais items to resolve."""
        for calais_item in itemlist:
            self.resolve(calais_item, test=test)

    def resolve(self, item, test=False):
        """Resolves an entity result of OpenCalais (or other) analysis to a 
        Freebase-defined entity"""

        if not test:
            print 'Emily resolution writing to database is not implemented!'
            return

        cur = self.conn.cursor()

        entity = item.data
        entity_type = item.type

        print 'Resolving', entity

        # Check if entity has known type.
        # This is done for disambiguation purposes, as it constrains Freebase's search.
        resolved_type = self._resolveEntityType(entity_type)
        if resolved_type is not None:
            print '\tType resolved to', resolved_type

        # Send freebase search with resolved type
        results = freebase.search(query=entity, type=resolved_type)

        # Dictionary keyed by entity, value indicates the number of times this
        # entity appears in the database.
        counts = {}

        # Consider each Freebase entity that was returned in this search for
        # disambiguation or inclusion in a semantic group.
        for result in results:
            eval = self._evaluateEntity(result)
            if eval is None:
                continue

            name, count = eval
            if name not in counts:
                counts[name] = 0
            counts[name] += count

        if len(counts) < 1:
            print '\tNot enough information to disambiguate against the articles db'
        else:
            print counts
            # Find key with the maximum value
            disambig = max(counts.iteritems(), key=operator.itemgetter(1))[0]
            print '\tCHOSE:', disambig 

        # TODO
        # We can also improve this method by using existing semantic sources 
        # about the civil war to ensure accurate disambiguation
        # eg. dbpedia - take everything in the 'Civil War' project and 
        # disambiguate against it.

        # TODO
        # Replace relevant database entries with a single new disambiguated 
        # entity.
        # for entity e in counts
        #   convert e to disambiguated name where it appears in entity table
        #   and
        #   update count column in entity table
        for entity in counts:
            query = 'UPDATE calais_items WHERE text=? SET text=?'
            params = (entity, name)

    def _evaluateEntity(self, result):
        """Evaluates a Freebase entity search result for disambiguation or
        inclusion in a semantic group."""
        score = result['relevance:score'] 
        if score < GROUPING_THRESHOLD:
            # Relevance score too low
            return None

        cur = self.conn.cursor()

        name = result['name']
        print '\t%s (%.2f) aliased:' % (name, score),

        # Construct a list with this Freebase entity's actual name and all
        # its aliases.
        search_terms = [name]
        search_terms.extend(result['alias'])

        # Loop through each of these possible names
        count = 0
        for term in search_terms:
            print term + ',',
            # Now count the number of times this query appears in the 
            # database in actual article text.

            # Note that this technique relies on the idea that we have a
            # good base of documents, because disambiguated possibilities
            # are verified by their presence in other documents.
            if not self.fts:
                query = 'SELECT * from articles WHERE text LIKE ?'
            else:
                # Look for exact string match
                query = 'SELECT * from articles_fts WHERE text MATCH ?'

            cur.execute(query, ('"' + term + '"',))
            count += len(cur.fetchall())
        print
        return name, count

    def _resolveEntityType(self, entity_type):
        resolved_type = None
        if entity_type is not None and entity_type in CALAIS_TYPE_MAPPING:
            # (support only for calais mapping only now)
            resolved_type = CALAIS_TYPE_MAPPING[entity_type]
        return resolved_type

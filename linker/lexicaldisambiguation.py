import re
import db.calaisitems

# Indicates whether or not these operations are on an FTS3 db, which is a 
# lot faster.
fts3 = True

class LexicalDisambiguator:
    """Basic tool for extracted entity disambiguation that uses basic
    string-based rules.

    eg. The following should resolve to the same entity:
        LINCOLN, Lincoln, Abe Lincoln
        Washington, Washington DC, Washington D.C.

    This sort of disambiguation is usually run before semantic disambiguation
    (ie. with Freebase), which will choose the best/fullest name for an
    entity.
    """

    def __init__(self, conn):
        self.conn = conn
        self.entities = None

    def _loadentities(self):
        """Loads a list of all entity type-value pairs."""
        cur = self.conn.cursor()
        cur.execute('SELECT * FROM calais_items')
        self.entities = db.calaisitems.processAll(cur.fetchall())

    def processAll(typeValuePairs, test=False):
        """Takes a list of type-value pairs."""
        for pair in typeValuePairs:
            self.process(pair, test=test)

    def process(typeValuePair, test=False):
        """Takes a type-value pair.
        eg. Person-Lincoln
        """
        type, value = typeValuePair

        # eg. substrings, different cases, without punctuation (washington d.c.)
        # normalize case, remove punctuation
        # TODO normalize entities
        value_normalized = re.sub('[\.-]', '', value.lower())

        # Records results that produced nonzero matches with normalized values
        results = []
        for item in self.entities:
            # Look matches on both type AND value are higher probability
            typematch = item.type == type

            # equivalence
            match = 0.0
            if item.data == value_normalized:
                match = 100.0
                
            # substring
            if item.data.find(value_normalized) > -1:
                match = float(len(value_normalized))/len(item.data) * 100.0
            else if value_normalized.find(item.data) > -1:
                match = float(len(item.data))/len(value_normalized) * 100.0

            # finished
            if match > 0.0:
                item.data = value_normalized
                results.append(item)

        # Write to db
        if test:
            print 'Not writing to db'
            return
        
        # TODO write to database

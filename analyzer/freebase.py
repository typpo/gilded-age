import freebase

# Percent Freebase relevance threshold to consider an entity for 
# disambiguation
RELEVANCE_THRESHOLD = 60.0

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

class FreebaseLinker:
    """Resolves ambiguous entities using Freebase
    """

    def resolve(self, entity, entity_type=None, test=False):
        """Resolves an entity result of OpenCalais (or other) analysis to a 
        Freebase-defined entity"""

        if not test:
            print 'Entity resolution writing to database is not implemented!'
            return

        # Check if entity has known type.
        # This is done for disambiguation purposes, as it constrains Freebase's search.
        resolved_type = self._resolveEntityType(entity_type)

        # Send freebase search with resolved type
        results = freebase.search(query=entity, type=resolved_type)

        # Dictionary keyed by entity, value indicates the number of times this
        # entity appears in the database.
        counts = {}

        for result in relevant_results:
            if result['relevance:score'] < RELEVANCE_THRESHOLD:
                # Relevance score too low
                continue

                name = result['name']

                # Construct a list with this entity's actual name and all its 
                # aliases
                search_terms = [name]
                search_terms.extend(result['alias'])

                # Loop through each of these possible names
                for term in search_terms:
                    # Now count the number of times this query appears in the 
                    # database in actual article text.
                    # TODO this would be helped a lot by FTS3
                    query = 'SELECT * from articles WHERE text LIKE ?'
                    if result no in counts:
                        counts[name] = 0
                    counts[name] += len(cur.execute(query, (term,)))

        # Now pick the entity that appeared the most.
        # TODO

        # Note that this technique relies on the idea that we have a
        # good base of documents, because disambiguated possibilities
        # are verified by their presence in other documents.

        # TODO
        # We can also improve this method by using existing semantic sources 
        # about the civil war to ensure accurate disambiguation (eg. dbpedia)

                # Then replace entry with main entity name.

    def _resolveEntityType(self, type):
        resolved_type = None
        if entity_type is not None and entity_type in CALAIS_TYPE_MAPPING:
            # (support only for calais mapping only now)
            resolved_type = CALAIS_TYPE_MAPPING[entity_type]
        return resolved_type

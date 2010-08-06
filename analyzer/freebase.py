import freebase

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
        resolved_type = None
        if entity_type is not None and entity_type in CALAIS_TYPE_MAPPING:
            # (support only for calais mapping only now)
            resolved_type = CALAIS_TYPE_MAPPING[entity_type]

        # Send freebase search with resolved type
        freebase.search(query=entity, type=resolved_type)

        # Try it without type mapping and compare top relevance?

        # Use alias to match others?

        # TODO send aliases with relevance > X to the database, 
        # and prefer the freebase entity that gets the most hits.
        # This sounds like the best idea but requires a good base of documents.
        # Which isn't too much to ask for, is it?

        # We can also improve this method by using existing semantic sources 
        # about the civil war to ensure accurate disambiguation (eg. dbpedia)

        # Then replace entry with main entity name.

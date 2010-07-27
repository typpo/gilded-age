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

    def resolve(self, entity, entity_type=None):
        # Check if entity has known type.
        # This is done for disambiguation purposes, as it constrains Freebase's search.
        resolved_type = None
        if entity_type is not None and entity_type in CALAIS_TYPE_MAPPING:
            # (support only for calais mapping only now)
            resolved_type = CALAIS_TYPE_MAPPING[entity_type]

        # Send freebase search with resolved type
        freebase.search(query=entity, type=resolved_type)
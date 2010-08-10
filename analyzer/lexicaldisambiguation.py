

class LexicalDisambiguator:
    """Basic tool for extracted entity disambiguation that uses basic
    string-based rules.

    eg. The following should resolve to the same entity:
        LINCOLN, Lincoln, Abe Lincoln
        Washington, Washington DC, Washington D.C.
    """
    # TODO would be really nice to use FTS3.

    def __init__(self, conn):
        self.conn = conn

    def disambiguate(pairs):
        """Takes type-value pairs.
        eg. Person-Lincoln
        """

        # Look for similar matches with both type AND value
        # This is a high probability match.

        

        # Look for similar matches with just value
        # Lower probability match

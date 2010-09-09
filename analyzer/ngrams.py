from db.articles import Article

class NGrams:
    """Extracts ngrams from text"""

    def __init__(self, conn):
        self.conn = conn

    def process(self, articles):
        """Extracts ngrams from article text"""
        pass

    def allngrams(self, n, text):
        """Returns ngrams of length up to n, starting at length 2"""
        for i in range(2, n+1):
            yield self.ngram(n, text)

    def ngram(self, n, text):
        """Returns ngrams of length n"""
        for i in range(len(text)-n+1):
            yield text[i:i+n]

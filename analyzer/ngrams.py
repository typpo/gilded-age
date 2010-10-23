from baseanalyzer import BaseAnalyzer
from db.articles import Article

class NGrams(BaseAnalyzer):
    """Extracts ngrams from text"""

    # Default max length of ngrams produced by execute() 
    defaultN = 7

    def __init__(self, conn):
        super(NGrams, self).__init__(conn)

    def execute(self, articles):
        """Extracts ngrams from article text.
        Returns a dictionary keyed by ngram with frequency as a value.
        """
        return self.execute(articles, {})

    def execute(self, articles, d):
        """Extracts ngrams from article text.  Adds to an existing dictionary.
        Returns a dictionary keyed by ngram with frequency as a value.
        """
        for article in articles:
            for ngram in self.allngrams(self.defaultN, article.text):
                if ngram not in d:
                    d[ngram] = 0
                d[ngram] += 1
        return d


    def allngrams(self, n, text):
        """Returns ngrams of length up to n, starting at length 2"""
        ret = []
        for i in range(2, n+1):
            ret.extend(self.ngram(n, text))
        return ret

    def ngram(self, n, text):
        """Returns ngrams of length n"""
        ret = []

        # Break into words
        words = text.split(' ')
        for i in range(len(words)-n+1):
            ret.append(words[i:i+n])
        return ret

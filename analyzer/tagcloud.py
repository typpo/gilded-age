from baseanalyzer import BaseAnalyzer
from graph import Graph
import db.articles
import math

FILTERS = 'analyzer/commonwords'    # file containing (100) common words
MAX_WORD_LEN = 3                    # reject words shorter than this

HTML_FONT_MIN = 8                   # minimum html font size, in pixels
HTML_FONT_MAX = 128                 # maximum html font size, in pixels

class TagCloud(BaseAnalyzer):
    """Makes a logarithmic tag cloud from article text."""

    def __init__(self, conn):
        super(TagCloud, self).__init__(conn)
        self.words = []
        self.loaded_common_words = False
        self.graph = None

    def execute(self, articles, use_title=False, use_entities=False):
        """
        Generates a tag cloud.

        use_title -- Parses article titles if true, otherwise uses articles 
            text.
        use_entities -- Performs a semantic analysis and creates tag cloud 
            based on extracted entities.
        """

        words = {}
        if use_entities:
            if self.graph is None:
                self.graph = Graph(self.conn)

            # TODO convert to MetaArticle
            articles_meta = self.graph.glomMetadata(articles)
            for meta in articles_meta:
                type = meta[10]                    
                data = meta[11]
                if type[0] != '_':
                    words[data] = 1 + words.get(data, 0)
        else:
            for article in articles:
                    splitme = article.title if use_title else article.text
                    tokens = splitme.split()
                    for token in tokens:
                        if token not in self.loadCommonWords() and \
                            (len(token) > MAX_WORD_LEN or MAX_WORD_LEN < 0):
                            words[token] = 1 + words.get(token, 0)

        return self.makeCloud(100,words.items())

    def loadCommonWords(self):
        if not self.loaded_common_words:
            f = open(FILTERS, 'r')
            self.words = f.read().split()
            f.close()
            self.loaded_common_words = True
        return self.words

    def writeHTML(self, cloudtable):
        range = HTML_FONT_MAX - HTML_FONT_MIN
        # Get max value
        max_count = cloudtable[max(cloudtable, key=lambda x: cloudtable.get(x))]

        # Write tag cloud
        f = open('tagcloud.html', 'w')
        for word, count in cloudtable.items():
            if count < 2:
                continue
            # TODO normalize by number of articles these words appear in...
            # otherwise we just get common entities like Congress

            # Write words with sizes spread out in tag cloud font size range
            f.write('<span style="font-size:%dpx">%s</span>' % \
                (HTML_FONT_MIN+count*range/max_count, word))
        f.close()

    def test(self):
        cur = self.conn.cursor()
        cur.execute('select * from articles_fts where text MATCH "corruption"')
        f=cur.fetchall()
        a=db.articles.processAll(f)
        q=self.execute(a, use_entities=True)

        self.writeHTML(q)

    def makeCloud(self, steps, input):
        """
        Logarithmic tag cloud
        Modified slightly from:
        http://www.chasedavis.com/2007/jan/16/log-based-tag-clouds-python/
        """
        temp, newThresholds = [], []
        results = {}
        for item in input:
            temp.append(item[1])
        maxWeight = float(max(temp))
        minWeight = float(min(temp))
        newDelta = (maxWeight - minWeight)/float(steps)
        for i in range(steps + 1):
           newThresholds.append((100 * math.log((minWeight + i * newDelta) + 2), i))
        for tag in input:
            fontSet = False
            for threshold in newThresholds[1:int(steps)+1]:
                if (100 * math.log(tag[1] + 2)) <= threshold[0] and not fontSet:
                    results[str(tag[0])] = threshold[1]
                    fontSet = True
        return results

def processAll(rows):
    """Takes a db response of many rows and returns a list of extracted article results."""
    ret = []
    for row in rows:
        ret.append(Article(row))
    return ret

class MetaArticle:
    """Takes a row in the articles db and parses it."""
    def __init__(self, row):
        self.id = row[0]
        self.source = row[1]
        self.alignment = row[2]
        self.page = row[3]
        self.title = row[4]
        self.summary = row[5]
        self.text = row[6]
        self.url = row[7]
        self.articleDate = row[8]
        self.timeEnter = row[9]
        self.relation_type = row[10]
        self.relation_data = row[11]
        self.relation_count = row[12]

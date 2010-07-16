def processAll(rows):
    """Takes a db response of many rows and returns a list of extracted article results."""
    ret = []
    for row in rows:
        ret.append(CalaisResult(row))
    return ret

class CalaisResult:
    """Takes a row in the calais results db and parses it."""
    def __init__(self, row):
        self.id = row[0]
        self.article_id = row[1]
        self.relation_id = row[2]
        self.relevance = row[3]
        self.has_full_text = row[4]

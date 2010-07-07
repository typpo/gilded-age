def processAll(rows):
    """Takes a db response of many rows and returns a list of extracted article results."""
    ret = []
    for row in rows:
        ret.append(CalaisItem(row))
    return ret

class CalaisItem:
    """Takes a row in the calais db and parses it."""
    def __init__(self, row):
        self.id = row[0]
        self.type = row[1]
        self.data = row[2]
        self.count = row[3]

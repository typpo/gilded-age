from db.articles import Article

class EntityGram:
    """Encapsulates entities contained in an article."""
    def __init__(self, article):
        self.article = article
        self.entities = entities

    def _extract(self):
        """Extracts entities from article"""
        pass

    def add(self, entity):
        self.entities.append(entity)

    def remove(self, entity):
        self.entities.remove(entity)

    def count(self):
        return len(self.entities)

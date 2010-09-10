from baseanalyzer import BaseAnalyzer
from freebaselinker import FreebaseLinker
from graph import Graph

class FreebaseAnalyzer(BaseAnalyzer):
    def __init__(self, conn, graph=None, linker=None):
        """Initialize with database connection and optional graph,
        Freebase linker"""

        print 'Initializing FreebaseAnalyzer...'
        super(FreebaseAnalyzer, self).__init__(conn)

        if graph is None:
            graph = Graph(conn)
        self.graph = graph

        if linker is None:
            linker = FreebaseLinker(conn)
        self.linker = linker

    def execute(self, documents):
        """Run analysis on a list of documents"""

        # Get list of calais results associated with documents
        for document in documents:
            entity_items = self.graph.getEntities(document)

        # Attempt to resolve them with freebase
        self.linker.resolveAll(entity_items)

from wikiCat.data.wikidata import WikiData
from graph_tool.all import *


class WikiGraph(WikiData):
    def __init__(self, project):
        WikiData.__init__(self, project, 'graph')
        self.graph = Graph()
        self.graph_file = '' # How to identify?
        pass


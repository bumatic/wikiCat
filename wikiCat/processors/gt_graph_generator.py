from wikiCat.processors.processor import Processor
from graph_tool.all import *

class GtGraphGenerator(Processor):
    def __init__(self, project, fixed='', errors=''):
        Processor.__init__(self, project, 'graph')
        self.basename = ''
        self.nodes = ''
        self.edges = ''
        self.events = ''

#
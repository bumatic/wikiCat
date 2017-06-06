from wikiCat.selectors.selector import Selector
from wikiCat.processors.PandasProcessor import PandasProcessor
import os


class SelectorCscore(PandasProcessor):
    def __init__(self, graph):
        self.graph = graph
        self.project = self.graph.project
        PandasProcessor.__init__(self, self.project, 'gt_graph')

        #SetVariables
        assert self.graph.curr_working_graph is not None, 'Error. Set a current working graph before creating ' \
                                                          'a selector.'
        self.nodes = self.graph.source_nodes
        self.nodes_location = self.graph.source_nodes_location
        self.events = self.graph.source_events
        self.events_location = self.graph.source_events_location
        self.edges = self.graph.source_edges
        self.edges_location = self.graph.source_edges_locations

    def get_highest_cscores(self, type, n=100):
        assert type == 'nodes' or type == 'edges' or type == 'events', 'Error. Pass one of the following types: nodes, edges, events.'
        if type == 'nodes':
            df = self.load_nodes(os.path.join(self.nodes_location, self.nodes))
        elif type == 'edges':
            df = self.load_edges(os.path.join(self.edges_location, self.edges))
        elif type == 'events':
            df = self.load_events(os.path.join(self.events_location, self.events))
        self.highest_cscores(df, n=10)



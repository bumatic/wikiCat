from wikiCat.processors.PandasProcessor import PandasProcessor
from wikiCat.processors.processor import Processor


class GtGraphProcessor(PandasProcessor):
    def __init__(self, graph):
        self.graph = graph
        self.project = self.graph.project
        PandasProcessor.__init__(self, self.project, 'gt_graph')
        # SetVariables
        assert self.graph.curr_working_graph is not None, 'Error. Set a current working graph before creating ' \
                                                          'a selector.'
        self.nodes = self.graph.source_nodes
        self.nodes_location = self.graph.source_nodes_location
        self.events = self.graph.source_events
        self.events_location = self.graph.source_events_location
        self.edges = self.graph.source_edges
        self.edges_location = self.graph.source_edges_location


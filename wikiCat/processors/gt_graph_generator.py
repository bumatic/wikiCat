from wikiCat.processors.pandas_processor_graph import PandasProcessorGraph
from graph_tool.all import *


class GtGraphGenerator(PandasProcessorGraph):
    def __init__(self, project, fixed='fixed_none', errors='errors_removed'):
        PandasProcessorGraph.__init__(self, project, 'graph')
        self.graph = Graph()

        # Create and internalize node property maps
        self.node_id = self.graph.new_vertex_property('string')
        self.graph.vertex_properties['id'] = self.node_id
        self.node_title = self.graph.new_vertex_property('string')
        self.graph.vertex_properties['title'] = self.node_title
        self.node_ns = self.graph.new_vertex_property('str')
        self.graph.vertex_properties['ns'] = self.node_ns
        self.node_cscore = self.graph.new_vertex_property('double')
        self.graph.vertex_properties['cscore'] = self.node_cscore

        # Create and internalize edge property maps
        self.edge_type = self.graph.new_edge_property('str')
        self.graph.edge_properties['type'] = self.edge_type
        self.edge_cscore = self.graph.new_edge_property('double')
        self.graph.edge_properties['cscore'] = self.edge_cscore
        self.node_id_dict = {}
        self.edge_list =[]

    def create_nodes(self, cscore=True):
        if cscore:
            self.load_nodes(['id', 'title', 'ns', 'cscore'])
        else:
            self.load_nodes(['id', 'title', 'ns'])
        node_count = len(self.nodes.index)
        node_iterator = self.nodes.iterrows()
        nlist = self.graph.add_vertex(node_count)
        for n in nlist:
            node = node_iterator.__next__()
            self.node_id_dict[node[0]] = n
            self.node_id[n] = node[0]
            self.node_title[n] = node[1]
            self.node_ns[n] = node[2]
            if cscore:
                self.node_cscore[n] = node[3]
            else:
                self.node_cscore[n] = 0

    def create_edges(self, cscore=True):
        if cscore:
            self.load_edges(['source', 'target', 'type', 'cscore'])
        else:
            self.load_edges(['source', 'target', 'type'])
        for edge in self.edges.iterrows():
            tmp = self.graph.add_edge(self.graph.vertex([self.node_id_dict[edge[0]]]), self.graph.vertex([self.node_id_dict[edge[1]]]))
            self.edge_type[tmp] = edge[2]
            if cscore:
                self.edge_cscore[tmp] = edge[3]
            else:
                self.edge_cscore[tmp] = 0
            self.edge_list.append(tmp)

    def save_graph_gt(self):
        # picking the filename and registering the file in the project needs to bee implemented.
        file = '123.gt'
        self.graph.save(file, fmt='gt')
        pass











from wikiCat.processors.pandas_processor_graph import PandasProcessorGraph
from graph_tool.all import *
import os
import shutil


class GtGraphGenerator(PandasProcessorGraph):
    def __init__(self, project, fixed='fixed_none', errors='errors_removed'):
        PandasProcessorGraph.__init__(self, project)
        self.graph = Graph()
        self.gt_filename = 'gt_graph_' + self.data_status + '.gt'
        #self.gt_edges_filename = 'gt_edges_'+self.data_status + '.json'
        self.gt_nodes_filename = 'gt_nodes_' + self.data_status + '.json'

        # Create and internalize node property maps
        self.node_id = self.graph.new_vertex_property('string')
        self.graph.vertex_properties['id'] = self.node_id
        self.node_title = self.graph.new_vertex_property('string')
        self.graph.vertex_properties['title'] = self.node_title
        self.node_ns = self.graph.new_vertex_property('string')
        self.graph.vertex_properties['ns'] = self.node_ns
        self.node_cscore = self.graph.new_vertex_property('double')
        self.graph.vertex_properties['cscore'] = self.node_cscore

        # Create and internalize edge property maps
        self.edge_type = self.graph.new_edge_property('string')
        self.graph.edge_properties['type'] = self.edge_type
        self.edge_cscore = self.graph.new_edge_property('double')
        self.graph.edge_properties['cscore'] = self.edge_cscore
        self.node_id_dict = {}
        #self.edge_dict = {}

        #self.fixed = fixed
        #self.errors = errors

    def create_gt_graph(self, cscore=True):
        self.create_nodes()
        self.create_edges()
        self.save_graph_gt()

    def create_nodes(self, cscore=True):
        # TODO Assumes that only one edges file exists. Needs fixing for inclusion of link_data
        if cscore:
            self.load_nodes(self.nodes_files[0], ['id', 'title', 'ns', 'cscore'])
        else:
            self.load_nodes(self.nodes_files[0], ['id', 'title', 'ns'])
        node_count = len(self.nodes.index)
        node_iterator = self.nodes.iterrows()
        nlist = self.graph.add_vertex(node_count)
        for n in nlist:
            node = node_iterator.__next__()
            self.node_id_dict[node[1]['id']] = int(node[0])
            self.node_id[n] = node[1]['id']
            self.node_title[n] = node[1]['title']
            self.node_ns[n] = node[1]['ns']
            if cscore:
                self.node_cscore[n] = node[1]['cscore']
            else:
                self.node_cscore[n] = 0

    def create_edges(self, cscore=True):
        #TODO Assumes that only one edges file exists. Needs fixing for inclusion of link_data
        counter = 0
        if cscore:
            self.load_edges(self.edges_files[0], ['source', 'target', 'type', 'cscore'])
        else:
            self.load_edges(self.edges_files[0], ['source', 'target', 'type'])
        for edge in self.edges.iterrows():

            if edge[1]['source'] in self.node_id_dict.keys() and edge[1]['target'] in self.node_id_dict.keys():
                #, self.graph.vertex(self.node_id_dict[]

                tmp = self.graph.add_edge(self.graph.vertex(self.node_id_dict[edge[1]['source']]), self.graph.vertex(self.node_id_dict[edge[1]['target']]))
                self.edge_type[tmp] = edge[1]['type']
                self.edge_type[tmp] = edge[1]['type']
                if cscore:
                    self.edge_cscore[tmp] = edge[1]['cscore']
                else:
                    self.edge_cscore[tmp] = 0
                #self.edge_dict[str(self.node_id_dict[edge[1]['source']]) + '|' + str(self.node_id_dict[edge[1]['target']])] = [self.node_id_dict[edge[1]['source']], self.node_id_dict[edge[1]['target']]]

            else:
                counter = counter + 1
        print('Number of Edges not created: '+ str(counter))

    def save_graph_gt(self):
        # registering the file in the project needs to bee implemented.
        self.graph.save(os.path.join(self.data_path, self.gt_filename), fmt='gt')
        self.write_json(os.path.join(self.data_path, self.gt_nodes_filename), self.node_id_dict)
        #self.write_json(os.path.join(self.data_path, self.gt_edges_filename), self.edge_dict)
        #self.add_gt_graph()
        pass

    def register_gt_graph(self):
        self.register_results('gt_graph', gt_file=self.gt_filename, gt_id_dict=self.gt_nodes_filename, nodes=self.nodes_files)



#
#        self.results_path = os.path.join(self.results_path, self.data_status)
#        if not os.path.isdir(self.results_path):
#            os.makedirs(self.results_path)
 ##       shutil.move(os.path.join(self.data_path, self.gt_filename), os.path.join(self.results_path, self.gt_filename))
  #      shutil.move(os.path.join(self.data_path, self.gt_nodes_filename), os.path.join(self.results_path, self.gt_nodes_filename))
   #     # shutil.move(os.path.join(self.data_path, self.gt_edges_filename), os.path.join(self.results_path, self.gt_edges_filename))
    #    for file in self.nodes_files:
     ##       shutil.copy(os.path.join(self.data_path, file), os.path.join(self.results_path, file))


        #TODO THIS NEEDS TO BE CHANGED TO REGISTER GT GRAPH AS GRAPH OBJECT
        #self.register_results('gt_graph', nodes=self.nodes_files, edges=self.edges_files, events=self.events_files,
        #                      gt=[self.gt_filename, self.gt_nodes_filename, self.gt_edges_filename],
        #                      fixed=self.fixed, errors=self.errors, override=True)
       # return










from wikiCat.data.data import Data
from graph_tool.all import *
import os
import shutil

class WikiGraph(Data):
    def __init__(self, project):
        Data.__init__(self, project, 'gt_graph')
        self.source_path = self.project.graph_data_path
        #self.graph = Graph()
        #self.graph_file = '' # How to identify?
        pass

    def add_new_graph(self, gt_file, gt_type, gt_id_dict, nodes):
        graph_path = os.path.join(self.data_path, gt_type, 'main')

        if gt_type in self.data.keys():
            print('Graph of this type already exists. Try creating a subgraph or adding it as another type')
            return self.data

        if not os.path.isdir(graph_path):
            os.makedirs(graph_path)

        shutil.move(os.path.join(self.source_path, gt_file), os.path.join(graph_path, gt_file))
        shutil.move(os.path.join(self.source_path, gt_id_dict), os.path.join(graph_path, gt_id_dict))
        if type(nodes) is list:
            for file in nodes:
                shutil.copy(os.path.join(self.source_path, file), os.path.join(graph_path, file))
        elif type(nodes) is str:
            shutil.copy(os.path.join(self.source_path, nodes), os.path.join(graph_path, nodes))

        self.data[gt_type] = {}
        self.data[gt_type]['main'] = {}
        self.data[gt_type]['main']['gt_file'] = gt_file
        self.data[gt_type]['main']['gt_node_id_file'] = gt_id_dict
        self.data[gt_type]['main']['nodes'] = nodes
        self.data[gt_type]['main']['location'] = graph_path
        print (self.data)
        return self.data




    '''

        if not os.path.isdir(self.results_path):
            os.makedirs(self.results_path)
        shutil.move(os.path.join(self.data_path, self.gt_filename), os.path.join(self.results_path, self.gt_filename))
        shutil.move(os.path.join(self.data_path, self.gt_nodes_filename), os.path.join(self.results_path, self.gt_nodes_filename))
        # shutil.move(os.path.join(self.data_path, self.gt_edges_filename), os.path.join(self.results_path, self.gt_edges_filename))
        for file in self.nodes_files:
            shutil.copy(os.path.join(self.data_path, file), os.path.join(self.results_path, file))


        #TODO THIS NEEDS TO BE CHANGED TO REGISTER GT GRAPH AS GRAPH OBJECT
        #self.register_results('gt_graph', nodes=self.nodes_files, edges=self.edges_files, events=self.events_files,
        #                      gt=[self.gt_filename, self.gt_nodes_filename, self.gt_edges_filename],
        #                      fixed=self.fixed, errors=self.errors, override=True)



    self.results_path = os.path.join(self.results_path, self.data_status)
        if not os.path.isdir(self.results_path):
            os.makedirs(self.results_path)
        shutil.move(os.path.join(self.data_path, self.gt_filename), os.path.join(self.results_path, self.gt_filename))
        shutil.move(os.path.join(self.data_path, self.gt_nodes_filename), os.path.join(self.results_path, self.gt_nodes_filename))
        shutil.move(os.path.join(self.data_path, self.gt_edges_filename), os.path.join(self.results_path, self.gt_edges_filename))
    '''

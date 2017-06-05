from wikiCat.data.data import Data
from wikiCat.selectors.selector import  Selector
from graph_tool.all import *
import os
import shutil


class WikiGraph(Data):
    def __init__(self, project, data={}, gt_type=None):
        Data.__init__(self, project, 'gt_graph')
        assert gt_type is not None, 'Error. gt_type needs to be passed!'
        self.gt_type = gt_type
        self.source_path = self.project.graph_data_path
        self.graph = Graph()
        self.data = data
        self.curr_working_graph = None
        self.curr_data_path = None
        self.source_nodes = None
        self.source_edges = None
        self.source_events = None
        self.source_location = None
        self.gt_wiki_id_map = None
        pass

    def add_new_graph(self, gt_file=None, gt_type='fixed_none__errors_removed', gt_wiki_id_map=None, gt_source="graph__fixed_none__errors_removed"):
        graph_path = os.path.join(self.data_path, gt_type, 'main')
        source_nodes = self.project.data_desc['graph'][gt_source]['nodes']
        source_edges = self.project.data_desc['graph'][gt_source]['edges']
        source_events = self.project.data_desc['graph'][gt_source]['events']

        if gt_type in self.data.keys():
            print('Graph of this type already exists. Try creating a subgraph or adding it as another type')
            return self.data

        if not os.path.isdir(graph_path):
            os.makedirs(graph_path)

        shutil.move(os.path.join(self.source_path, gt_file), os.path.join(graph_path, gt_file))
        shutil.move(os.path.join(self.source_path, gt_wiki_id_map), os.path.join(graph_path, gt_wiki_id_map))

        self.data = {}
        self.data['main'] = {}
        self.data['main']['gt_file'] = gt_file
        self.data['main']['gt_wiki_id_map'] = gt_wiki_id_map
        self.data['main']['location'] = graph_path
        self.data['main']['source_nodes'] = source_nodes
        self.data['main']['source_edges'] = source_edges
        self.data['main']['source_events'] = source_events
        self.data['main']['source_location'] = self.source_path
        return self.data

    '''
    def assemble_source_locations(self, files):
        if type(files) is list:
            for i in range(len(files)):
                files[i] = os.path.join(self.source_path, files[i])
        else:
            files = os.path.join(self.source_path, files)
        return files
    '''

    def load_graph(self, type='main'):
        #TODO probably does not work?
        try:
            self.graph.load(self.data[self.curr_working_graph][type]['gt_file'])
        except:
            print('Graph could not be loaded. A valid current working graph needs to be set before loading.')

    def list_graphs(self):
        print('Keys of available Graphs:')
        for key in self.data:
            print(key)

    def set_working_graph(self, key='main'):
        self.curr_working_graph = key

        if 'source_location' in self.data[key].keys():
            self.source_location = self.data[key]['source_location']
        else:
            self.source_location = self.data[self.data[key]['derived_from']]['source_location']
        if 'source_nodes' in self.data[key].keys():
            self.source_nodes = self.data[key]['source_nodes']
        else:
            self.source_nodes = self.data[self.data[key]['derived_from']]['source_nodes']
        if 'source_edges' in self.data[key].keys():
            self.source_edges = self.data[key]['source_edges']
        else:
            self.source_edges = self.data[self.data[key]['derived_from']]['source_edges']
        if 'source_events' in self.data[key].keys():
            self.source_events = self.data[key]['source_events']
        else:
            self.source_events = self.data[self.data[key]['derived_from']]['source_events']
        if 'gt_wiki_id_map' in self.data[key].key():
            self.gt_wiki_id_map = self.data[key]['gt_wiki_id_map']
        else:
            self.id_map = self.data[self.data[key]['derived_from']]['gt_wiki_id_map']
        self.curr_data_path = self.data[key]['location']

    def generate_snapshots(self, slice, cscore=True, start_date=None, end_date=None):
        pass

    def create_subgraph(self, title):
        pass

    def update_graph_data(self, data):
        assert type(data) is dict, 'Error: Data for updating a graph needs to be passed as dict.'
        self.data = data
        self.project.update_gt_graph_desc(self.gt_type, self.data)





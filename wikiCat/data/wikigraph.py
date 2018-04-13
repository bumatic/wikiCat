from wikiCat.data.data import Data
from wikiCat.processor.gt_graph_generator import GtGraphGenerator
from wikiCat.processor.gt_sub_graph_processor import SubGraphProcessor
from wikiCat.selector.selector_sub_graph import SubGraph
from wikiCat.selector.selector_snapshots import Snapshots
from wikiCat.selector.selector_cscore import SelectorCscore
from wikiCat.processor.oldest_revision import OldestRevision


# from wikiCat.selector.reworked_selector import Selector
from graph_tool.all import *
#import os
#import shutil


class WikiGraph(Data):
    def __init__(self, project):
        Data.__init__(self, project, 'gt_graph')
        #self.source_path = self.project.pinfo['path']['graph']
        self.graph = Graph()

        if 'main' in self.project.pinfo['gt_graph'].keys():
            curr_data = self.project.pinfo['gt_graph']['main']
            self.curr_working_graph = 'main'
            self.curr_data_path = curr_data['location']
            self.source_nodes = curr_data['source_nodes']
            self.source_nodes_location = curr_data['source_location']
            self.source_edges = curr_data['source_edges']
            self.source_edges_location = curr_data['source_location']
            self.source_events = curr_data['source_events']
            self.source_events_location = curr_data['source_location']
            self.source_location = curr_data['source_location']
            self.gt_file = curr_data['gt_file']
            self.gt_wiki_id_map = curr_data['gt_wiki_id_map']
            self.gt_wiki_id_map_location = curr_data['location']
        else:
            self.curr_working_graph = None
            self.curr_data_path = None
            self.source_nodes = None
            self.source_nodes_location = None
            self.source_edges = None
            self.source_edges_location = None
            self.source_events = None
            self.source_events_location = None
            self.source_location = None
            self.gt_file = None
            self.gt_wiki_id_map = None
            self.gt_wiki_id_map_location = None

    def load_graph(self, type='main'):
        try:
            self.graph.load(self.gt_file)
        except:
            print('Graph could not be loaded. A valid current working graph needs to be set before loading.')

    def list_graphs(self):
        print('Keys of available Graphs:')
        for key in self.data:
            print(key)

    def get_working_graph(self):
        return self.curr_working_graph

    def set_working_graph(self, key='main'):
        self.curr_working_graph = key
        self.curr_data_path = self.project.pinfo['gt_graph'][key]['location']
        if key == 'main':
            curr_data = self.project.pinfo['gt_graph'][key]
            self.source_location = curr_data['source_location']
            self.source_nodes_location = self.source_location
            self.source_edges_location = self.source_location
            self.source_events_location = self.source_location
            self.source_nodes = curr_data['source_nodes']
            self.source_edges = curr_data['source_edges']
            self.source_events = curr_data['source_events']
            self.gt_file = curr_data['gt_file']
            self.gt_wiki_id_map = curr_data['gt_wiki_id_map']
            self.gt_wiki_id_map_location = curr_data['location']

        else:
            curr_data = self.project.pinfo['gt_graph'][key]
            # print(curr_data)
            if 'gt_file' in curr_data.keys():
                self.gt_file = curr_data['gt_file']
            if 'source_location' in curr_data.keys():
                self.source_location = curr_data['source_location']
            else:
                self.source_location = self.project.pinfo['gt_graph'][curr_data['derived_from']]['source_location']
            if 'source_nodes' in curr_data.keys():
                self.source_nodes = curr_data['source_nodes']
                self.source_nodes_location = self.curr_data_path
            else:
                self.source_nodes = self.project.pinfo['gt_graph'][curr_data['derived_from']]['source_nodes']
                self.source_nodes_location = self.source_location
            if 'source_edges' in curr_data.keys():
                self.source_edges = curr_data['source_edges']
                self.source_edges_location = self.curr_data_path
            else:
                self.source_edges = self.project.pinfo['gt_graph'][curr_data['derived_from']]['source_edges']
                self.source_edges_location = self.source_location
            if 'source_events' in curr_data.keys():
                self.source_events = curr_data['source_events']
                self.source_events_location = self.curr_data_path
            else:
                self.source_events = self.project.pinfo['gt_graph'][curr_data['derived_from']]['source_events']
                self.source_events_location = self.source_location
            if 'gt_wiki_id_map' in curr_data.keys():
                self.gt_wiki_id_map = curr_data['gt_wiki_id_map']
                self.gt_wiki_id_map_location = self.curr_data_path
            else:
                self.gt_wiki_id_map = self.project.pinfo['gt_graph'][curr_data['derived_from']]['gt_wiki_id_map']
                self.gt_wiki_id_map_location = self.project.pinfo['gt_graph'][curr_data['derived_from']]['location']

    def generate_gt_graph(self):
        gt_graph_generator = GtGraphGenerator(self.project)
        gt_graph_generator.create_gt_graph()

    def create_subgraph(self, title=None, seed=None, cats=True, subcats=1, supercats=1,
                        links=False, inlinks=2, outlinks=2):
        SubGraph(self.project).create(title=title, seed=seed, cats=cats, subcats=subcats, supercats=supercats,
                                      links=links, inlinks=inlinks, outlinks=outlinks)
        self.set_working_graph(key=title)
        SubGraphProcessor(self.project).create_gt_subgraph()

    def create_gt_subgraph(self, title=None):
        self.set_working_graph(key=title)
        SubGraphProcessor(self.project).create_gt_subgraph()

    def create_snapshots(self, graph=None, title='Snapshots', slice='year', cscore=True,
                         start_date='2003-01-01', end_date=None):
        if graph is not None:
            self.set_working_graph(graph)
        Snapshots(self.project).create(title=title, slice=slice, cscore=cscore, start_date=start_date, end_date=end_date)
        SubGraphProcessor(self.project).internalize_snapshots(title)

        # Funktioniert noch nicht. Außerdem: MACHT das hier Sinn? Oder doch lieber in graph objekt????
    #def create_subgraph(self, title=None, seed=[], cats=True, subcats=1, supercats=1):
    #    SubGraph(self).create(title, seed, cats, subcats, supercats)
    #    self.graph.set_working_graph(title)
    #    SubGraphProcessor(self).create_gt_subgraph()
    #    Snapshots(self).create(title)
    #    SubGraphProcessor.internalize_snapshots(title)

    def update_graph_data(self, data):
        # print (data)
        # print (self.curr_working_graph)
        assert type(data) is dict, 'Error: Data for updating a graph needs to be passed as dict.'
        self.data = data
        self.project.update_gt_graph_data(self.data) #self.curr_working_graph,

    def get_highest_cscores(self, cscore_type, n=100, cats_only=False, save=True):
        SelectorCscore(self.project).get_highest_cscores(cscore_type, n=n, cats_only=cats_only, save=save)


    '''
    def add_new_graph(self, gt_file=None, gt_wiki_id_map=None, gt_source="graph__fixed_none__errors_removed"):
        graph_path = os.path.join(self.data_path, 'main')
        source_nodes = self.project.pinfo['data']['graph']['nodes'] #data_desc['graph'][gt_source]['nodes']
        source_edges = self.project.pinfo['data']['graph']['edges'] #data_desc['graph'][gt_source]['edges']
        source_events = self.project.pinfo['data']['graph']['events'] #data_desc['graph'][gt_source]['events']

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

    '''
    def assemble_source_locations(self, files):
        if type(files) is list:
            for i in range(len(files)):
                files[i] = os.path.join(self.source_path, files[i])
        else:
            files = os.path.join(self.source_path, files)
        return files
    '''



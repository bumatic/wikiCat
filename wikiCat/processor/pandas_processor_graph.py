from wikiCat.processor.processor import Processor
import pandas as pd
import os


class PandasProcessorGraph(Processor):
    # TODO check where this is used
    def __init__(self, project):
        Processor.__init__(self, project, 'graph')
        # self.project = self.project
        self.path = self.project.pinfo['path']['graph']
        # self.data_status = 'graph__' + fixed + '__' + errors

        if 'events' in self.project.pinfo['data']['graph'].keys():
            self.events_files = self.project.pinfo['data']['graph']['events']
            self.events = pd.DataFrame()
        else:
            print('No csv with events available')
        if 'nodes' in self.project.pinfo['data']['graph'].keys():
            self.nodes_files = self.project.pinfo['data']['graph']['nodes']
            self.nodes = pd.DataFrame()
        else:
            print('No csv with nodes available')
        if 'edges' in self.project.pinfo['data']['graph'].keys():
            self.edges_files = self.project.pinfo['data']['graph']['edges']
            self.edges = pd.DataFrame()
        else:
            print('No csv with edges available')

        #if 'gt' in self.data_obj.data[self.data_status]:
        #    self.gt_file = self.data_obj.data[self.data_status]['gt']
        #else:
        #    print('No graph_tool gt file available')

    def load_events(self, file, columns=[]):
        # Default events columns: ['source', 'target', 'revision' 'event', ('cscore)]
        self.events = pd.read_csv(os.path.join(self.path, file), header=None, delimiter='\t',
                                  names=columns)

    def load_edges(self, file, columns=[]):
        # Default edge columns ['source', 'target', 'type', ('cscore')]
        self.edges = pd.read_csv(os.path.join(self.path, file), header=None, delimiter='\t',
                                 names=columns)

    def load_nodes(self, file, columns=[]):
        # Default node columns ['id', 'title', 'ns', ('cscore')]
        self.nodes = pd.read_csv(os.path.join(self.path, file), header=None, delimiter='\t',
                                 names=columns)




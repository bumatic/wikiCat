from wikiCat.processors.processor import Processor
import pandas as pd
import os


class PandasProcessorGraph(Processor):
    def __init__(self, project, fixed='fixed_none', errors='errors_removed'):
        Processor.__init__(self, project, 'graph')

        self.project = self.project
        self.fixed = fixed
        self.errors = errors
        self.data_status = 'graph__' + fixed + '__' + errors
        self.events_file = self.data_obj.data['events']
        self.events = pd.DataFrame()
        self.nodes_file = self.data_obj.data['nodes']
        self.nodes = pd.DataFrame()
        self.edges_file = self.data_obj.data['edges']
        self.edges = pd.DataFrame()

    def load_events(self, columns=[]):
        self.events = pd.read_csv(os.path.join(self.path, self.events_file), header=None, delimiter='\t',
                                  names=columns)

    def load_edges(self, columns=[]):
        self.edges = pd.read_csv(os.path.join(self.path, self.edges_file), header=None, delimiter='\t',
                                 names=columns)

    def load_nodes(self, columns=[]):
        self.nodes = pd.read_csv(os.path.join(self.path, self.nodes_file), header=None, delimiter='\t',
                                 names=columns)

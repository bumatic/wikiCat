from wikiCat.processor.gt_graph_processor import GtGraphProcessor
from wikiCat.processor.pandas_processor import PandasProcessor
import pandas as pd
import os


class SelectorCscore(GtGraphProcessor, PandasProcessor):
    def __init__(self, graph):
        GtGraphProcessor.__init__(self, graph)
        PandasProcessor.__init__(self, self.project, 'gt_graph')

        '''
        #SetVariables
        assert self.graph.curr_working_graph is not None, 'Error. Set a current working graph before creating ' \
                                                          'a selector.'
        self.nodes = self.graph.source_nodes
        self.nodes_location = self.graph.source_nodes_location
        self.events = self.graph.source_events
        self.events_location = self.graph.source_events_location
        self.edges = self.graph.source_edges
        self.edges_location = self.graph.source_edges_location
        '''

    def get_highest_cscores(self, type, n=100, cats_only=False):
        assert type == 'nodes' or type == 'edges' or type == 'events', 'Error. Pass one of the following types: nodes, edges, events.'

        df_nodes = self.load_nodes(os.path.join(self.nodes_location, self.nodes[0]))
        df_edges = self.load_edges(os.path.join(self.edges_location, self.edges[0]))
        df_source = df_edges[['source']]
        df_source.columns = ['id']
        df_target = df_edges[['target']]
        df_target.columns = ['id']
        df_ids = df_source.append(df_target)

        if type == 'nodes':
            df = pd.merge(df_nodes, df_ids, how='inner', left_on='id', right_on='id')
            if cats_only:
                df = df.loc[df['ns'] == 14]
        elif type == 'edges':
            df = pd.merge(df_edges, df_nodes, how='inner', left_on='source', right_on='id')
            df = df[['title', 'target', 'type', 'cscore_x']]
            df = pd.merge(df, df_nodes, how='inner', left_on='target', right_on='id')
            df = df[['title_x', 'title_y', 'type', 'cscore_x']]
            df.columns = ['title_source', 'title_target', 'type', 'cscore']
            if cats_only:
                df = df.loc[df['type'] == 'cat']
        elif type == 'events':
            df_events = self.load_events(os.path.join(self.events_location, self.events[0]))
            df = pd.merge(df_events, df_nodes, how='inner', left_on='source', right_on='id')
            df = df[['revision', 'title', 'target', 'event', 'cscore_x']]
            df = pd.merge(df, df_nodes, how='inner', left_on='target', right_on='id')
            df = df[['revision', 'title_x', 'title_y', 'event', 'cscore_x']]
            df.columns = ['revision', 'title_source', 'title_target', 'event', 'cscore']

        df = df.drop_duplicates()
        self.highest_cscores(df, n=n)



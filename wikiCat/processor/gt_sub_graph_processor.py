from wikiCat.processor.gt_graph_processor import GtGraphProcessor
from graph_tool.all import *
import pandas as pd
import os
import pprint


class SubGraphProcessor(GtGraphProcessor):
    def __init__(self, project):
        GtGraphProcessor.__init__(self, project)
        self.gt = Graph()
        self.gt_filename = None
        self.data = self.graph.data
        self.working_graph = self.graph.curr_working_graph
        self.working_graph_path = self.graph.data[self.working_graph]['location']
        self.results_path = os.path.join(self.project.pinfo['path']['results'], self.working_graph)
        self.gt_wiki_id_map_path, self.gt_wiki_id_map_file = self.find_gt_wiki_id_map()
        self.gt_wiki_id_map = pd.read_csv(os.path.join(self.gt_wiki_id_map_path, self.gt_wiki_id_map_file),
                                          header=None, delimiter='\t', names=['wiki_id', 'gt_id'], na_filter=False)
        #print(self.gt_wiki_id_map)
        #print(self.gt_wiki_id_map_file)
        #print(self.gt_wiki_id_map_path)

        # Print all class variables
        #self.pp = pprint.PrettyPrinter(indent=4)
        #v = vars(self)
        #self.pp.pprint(v)

    def find_gt_wiki_id_map(self):
        if 'gt_wiki_id_map' in self.data[self.working_graph].keys():
            file = self.data[self.working_graph]['gt_wiki_id_map']
            path = self.working_graph_path
        else:
            print('LOAD SUPER MAP')
            file = self.data['main']['gt_wiki_id_map']
            path = self.data['main']['location']
        return path, file

    def internalize_snapshots(self, stype):
        if self.check_gt():
            self.load()
            print('GRAPH LOADED')
        else:
            print('Graph is derived from a super graph. Create a standalone graph first before internalizing views.')
            return
        snapshot_files = self.data[self.working_graph][stype]['files']
        print(snapshot_files)
        snapshot_path = os.path.join(self.graph.data[self.working_graph]['location'], stype)
        for file in snapshot_files:
            prop_map = self.gt.new_edge_property('bool')
            property_map_name = stype + '_' + str(file[:-6])
            df = pd.read_csv(os.path.join(snapshot_path, file), header=None, delimiter='\t',
                             names=['source', 'target']) #, na_filter=False
            print(df)
            #TODO Snapshots IDs werden bei erzeugen der Snapshots resolved. dies verusracht fehler, wenn eigener GT Graph erzeugt wird. Dann müssen die SNAPSHOTS NOCHMAL ERSTELLT WERDEN.
            df = self.resolve_ids(df) # aktuell verursacht das fehler in main, da hier die IDs schon resolved sind.
            print('RESOLVED IDs')
            #for v in self.gt.vertices():
            #    print(v)
            #    print(self.gt.vp.id[v])
            #    print(self.gt.vp.title[v])
            print(df)
            for key, item in df.iterrows():
                #print(key)
                #print(item)
                prop_map[self.gt.edge(item['source'], item['target'])] = True
            self.gt.edge_properties[property_map_name] = prop_map
        if self.gt_filename is None:
            self.gt_filename = self.working_graph + '.gt'
        self.gt.save(os.path.join(self.working_graph_path, self.gt_filename), fmt='gt')

    def create_gt_subgraph(self):
        gt_exists = self.check_gt()
        print('gt exists?')
        print(gt_exists)
        if not gt_exists:
            self.load()
            print('GRAPH LOADED, START CREATING THE VIEW')
            self.gt = GraphView(self.gt, vfilt=lambda v: v.out_degree() > 0 or v.in_degree() > 0)
            print('VIEW CREATED. START PURGING EDGES AND VERTICES')
            self.gt.purge_vertices()
            #self.gt.purge_edges()
            print('PURGING DONE, START CREATING THE WIKI ID MAP')
            gt_wiki_id_map = []
            self.gt_wiki_id_map_file = self.working_graph + '_gt_wiki_id_map.csv'
            for v in self.gt.vertices():
                gt_wiki_id_map.append([self.gt.vp.id[v], v])
            self.write_list(os.path.join(self.working_graph_path, self.gt_wiki_id_map_file), gt_wiki_id_map)
            if self.gt_filename is None:
                self.gt_filename = self.working_graph + '.gt'
            self.save_gt_graph()
            print('DONE. GT FILE OF SUBGRAPH CREATED. IDs CHANGED! SNAPSHOTS AND OTHER STUFF NEED TO BE RECREATED')
        else:
            print('GT file for subgraph already exists.')
            return

    def check_gt(self):
        if 'gt_file' in self.data[self.working_graph].keys():
            return True
        else:
            return False

    def load(self):
        if self.working_graph != 'main':
            if 'gt_file' in self.data[self.working_graph].keys():
                self.gt.load(
                    os.path.join(self.data[self.working_graph]['location'], self.data[self.working_graph]['gt_file']))
                self.gt_filename = self.data[self.working_graph]['gt_file']
            else:
                print('CREATE VIEW FROM SUPER-GRAPH. START LOADING SUPER-GRAPH') # print(self.data[self.working_graph]['derived_from'])
                super_graph = self.data[self.working_graph]['derived_from']
                self.gt.load(os.path.join(self.data[super_graph]['location'], self.data[super_graph]['gt_file']))
                print('SUPER-GRAPH LOADED.')
                graph_view = self.create_gt_view(self.edges_location, self.edges[0])
                self.gt = graph_view
                #print(self.gt)
        else:
            self.gt.load(os.path.join(self.data[self.working_graph]['location'], self.data[self.working_graph]['gt_file']))
            self.gt_filename = self.data[self.working_graph]['gt_file']

    def create_gt_view(self, path, file):
        print('START CREATING GT VIEW FROM SUPER-GRAPH')
        prop_map = self.gt.new_edge_property('bool')
        '''
        dtype = {   
            'source': int,
            'target': int,
            'type': str,
            'cscore': float
        }
        '''
        print(os.path.join(path, file))
        df = pd.read_csv(os.path.join(path, file), header=None, delimiter='\t',
                         names=['source', 'target', 'type', 'cscore'], na_filter=False)
        df = df[['source', 'target']]
        print(df)
        df = self.resolve_ids(df)
        print(df)
        for key, item in df.iterrows():
            print(key)
            print(item)
            prop_map[self.gt.edge(item['source'], item['target'])] = True
        graph_view = GraphView(self.gt, efilt=prop_map)
        graph_view = GraphView(graph_view, vfilt=lambda v: v.out_degree() > 0 or v.in_degree() > 0)
        return graph_view

    def resolve_ids(self, df):
        #print(df)
        #print(self.gt_wiki_id_map)
        df = pd.merge(df, self.gt_wiki_id_map, how='inner', left_on='source', right_on='wiki_id')
        df = df[['gt_id', 'target']]
        #print('resolved df')
        #print(df)
        df.columns = ['source', 'target']
        df = pd.merge(df, self.gt_wiki_id_map, how='inner', left_on='target', right_on='wiki_id')
        df = df[['source', 'gt_id']]
        df.columns = ['source', 'target']
        return df

    def save_gt_graph(self):
        self.gt.save(os.path.join(self.working_graph_path, self.gt_filename), fmt='gt')
        self.update_graph_data(self.gt_filename)

    def update_graph_data(self, filename):
        new_graph_data = self.data
        new_graph_data[self.working_graph]['gt_file'] = filename
        new_graph_data[self.working_graph]['gt_wiki_id_map'] = self.gt_wiki_id_map_file
        self.graph.update_graph_data(new_graph_data)

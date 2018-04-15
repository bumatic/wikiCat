from wikiCat.processor.pandas_processor_graph import PandasProcessorGraph
from graph_tool.all import *
import os
import shutil


class GtGraphGenerator(PandasProcessorGraph):
    def __init__(self, project):  # , fixed='fixed_none', errors='errors_removed'
        PandasProcessorGraph.__init__(self, project)
        self.graph = Graph()
        self.gt_filename = 'gt_graph.gt'
        self.gt_nodes_filename = 'gt_nodes_map.csv'
        self.data_path = os.path.join(self.data_path, 'main')

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
        self.node_id_list = []

    def create_gt_graph(self, cscore=True):
        self.create_nodes(cscore=cscore)
        self.create_edges(cscore=cscore)
        self.save_graph_gt()
        self.register_gt_graph()

    def load_relevant_nodes(self, cscore=True):
        if cscore:
            self.load_nodes(self.nodes_files[0], ['id', 'title', 'ns', 'cscore'])
        else:
            self.load_nodes(self.nodes_files[0], ['id', 'title', 'ns'])
        if cscore:
            self.load_edges(self.edges_files[0], ['source', 'target', 'type', 'cscore'])
        else:
            self.load_edges(self.edges_files[0], ['source', 'target', 'type'])

        edge_source = self.edges['source']
        edge_source.columns = ['id']
        edge_target = self.edges['target']
        edge_target.columns = ['id']
        relevant_ids = edge_source.append(edge_target)
        relevant_ids = relevant_ids.drop_duplicates()
        print('Total number of nodes:')
        print(len(self.nodes))
        self.nodes = self.nodes[self.nodes['id'].isin(list(relevant_ids))]
        print('Number of relevant nodes:')
        print(len(self.nodes))

    def create_nodes(self, cscore=True):
        # TODO Assumes that only one edges file exists. Needs fixing for inclusion of link_data
        '''
        if cscore:
            # dTYPE Currently not used in load.nodes
            dtype = {
                'id': int,
                'title': str,
                'ns': int,
                'cscore': float
            }
            self.load_nodes(self.nodes_files[0], ['id', 'title', 'ns', 'cscore'], dtype)
        else:
            # Currently not used in load.nodes
            dtype = {
                'id': int,
                'title': str,
                'ns': int
            }
            self.load_nodes(self.nodes_files[0], ['id', 'title', 'ns'], dtype)
        '''
        self.load_relevant_nodes(cscore=cscore)
        #print(self.nodes.index)
        node_count = len(self.nodes.index)
        node_iterator = self.nodes.iterrows()
        nlist = self.graph.add_vertex(node_count)
        counter = 0
        for n in nlist:
            counter += 1
            self.nodes = self.nodes.reset_index(drop=True)
            print(self.nodes)
            node = node_iterator.__next__()
            #print(node)

            self.node_id_list.append([node[1]['id'], int(node[0])])
            self.node_id_dict[node[1]['id']] = int(node[0])


            self.node_id[n] = node[1]['id']
            self.node_title[n] = node[1]['title']
            self.node_ns[n] = node[1]['ns']
            if cscore:
                self.node_cscore[n] = node[1]['cscore']
            else:
                self.node_cscore[n] = 0
        print('Nodes created in gt graph: ' + str(counter))

    def create_edges(self, cscore=True):
        #TODO Assumes that only one edges file exists. Needs fixing for inclusion of link_data
        counter = 0
        if cscore:
            self.load_edges(self.edges_files[0], ['source', 'target', 'type', 'cscore'])
        else:
            self.load_edges(self.edges_files[0], ['source', 'target', 'type'])
        #print(self.edges)
        print(self.node_id_dict)
        for edge in self.edges.iterrows():
            print(edge[1]['source'])
            print(type(edge[1]['source']))
            print(self.node_id_dict[edge[1]['source']])
            if not edge[1]['source'] in self.node_id_dict.keys() or not edge[1]['target'] in self.node_id_dict.keys():
                if not edge[1]['source'] in self.node_id_dict.keys():
                    #print('Source missing')
                    #print(edge[1]['source'])
                    pass
                if not edge[1]['target'] in self.node_id_dict.keys():
                    #print('target missing')
                    #print(edge[1]['target'])
                    pass

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
        print('Number of Edges not created: ' + str(counter))

    def save_graph_gt(self):
        # registering the file in the project needs to bee implemented.

        if not os.path.isdir(os.path.join(self.project.pinfo['path']['gt_graph'], 'main')):
            os.mkdir(os.path.join(self.project.pinfo['path']['gt_graph'], 'main'))

        self.graph.save(os.path.join(self.project.pinfo['path']['gt_graph'], 'main', self.gt_filename), fmt='gt')
        self.write_list(os.path.join(self.project.pinfo['path']['gt_graph'], 'main', self.gt_nodes_filename), self.node_id_list)
        # self.write_json(os.path.join(self.data_path, self.gt_edges_filename), self.edge_dict)
        # self.add_gt_graph()
        pass

    def register_gt_graph(self):
        results = {
            'gt_file': self.gt_filename,
            'gt_wiki_id_map': self.gt_nodes_filename,
            'location': os.path.join(self.project.pinfo['path']['gt_graph'], 'main'),
            'source_nodes': self.project.pinfo['data']['graph']['nodes'],
            'source_events': self.project.pinfo['data']['graph']['events'],
            'source_edges': self.project.pinfo['data']['graph']['edges'],
            'source_location': self.project.pinfo['path']['graph']
        }
        self.register_gt_results('main', results)



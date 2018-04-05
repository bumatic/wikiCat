import os
import pandas as pd
import json
from graph_tool.all import *
from wikiCat.processor.gt_graph_processor import GtGraphProcessor


class Visualizer(GtGraphProcessor):
    def __init__(self, graph):
        GtGraphProcessor.__init__(self, graph)
        self.gt = Graph()
        self.data = self.graph.data
        self.working_graph = self.graph.curr_working_graph
        self.working_graph_path = self.graph.data[self.working_graph]['location']
        self.results_path = os.path.join(self.project.pinfo['path']['results'], self.working_graph)
        self.gt_wiki_id_map_path, self.gt_wiki_id_map_file = self.find_gt_wiki_id_map()
        self.gt_wiki_id_map = pd.read_csv(os.path.join(self.gt_wiki_id_map_path, self.gt_wiki_id_map_file),
                                          header=None, delimiter='\t', names=['wiki_id', 'gt_id'])
        #self.drawing_props = {}

        # Print all class variables
        # self.pp = pprint.PrettyPrinter(indent=4)
        # v = vars(self)
        # self.pp.pprint(v)
        # self.pp.pprint(self.drawing_props)
        print('Initialized graph visualizer')

    def find_gt_wiki_id_map(self):
        if 'gt_wiki_id_map' in self.data[self.working_graph].keys():
            file = self.data[self.working_graph]['gt_wiki_id_map']
            path = self.working_graph_path
        else:
            file = self.data['main']['gt_wiki_id_map']
            path = self.data['main']['location']
        return path, file

    def snapshots(self, stype, drawing_props_file=None):
        self.load()
        self.results_path = os.path.join(self.results_path, stype)
        snapshot_files = self.data[self.working_graph][stype]['files']
        snapshot_path = os.path.join(self.graph.data[self.working_graph]['location'], stype)
        for file in snapshot_files:
            if drawing_props_file is not None:
                drawing_props = self.load_drawing_props(os.path.join(self.project.pinfo['path']['gt_props'], drawing_props_file))
            else:
                drawing_props = self.load_drawing_props(os.path.join('wikiCat', 'visualizer', 'drawing_default.json'))
            graph_view = self.create_snapshot_view(snapshot_path, file, stype)
            self.visualize(graph_view, file[:-4], drawing_props)

    def create_snapshot_view(self, path, file, stype):
        prop_map = stype + '_' + str(file[:-6])
        if prop_map in self.gt.edge_properties.keys():
            prop_map = self.gt.edge_properties[prop_map]
        else:
            self.gt.list_properties()
            prop_map = self.gt.new_edge_property('bool')
            df = self.load_edges(os.path.join(path, file))
            if len(df.index) > 0:
                for key, item in df.iterrows():
                    prop_map[self.gt.edge(item['source'], item['target'])] = True
        graph_view = GraphView(self.gt, efilt=prop_map)
        graph_view = GraphView(graph_view, vfilt=lambda v: v.out_degree() > 0 or v.in_degree() > 0)
        return graph_view

    def load(self):
        if self.working_graph != 'main':
            if 'gt_file' in self.data[self.working_graph].keys():
                self.gt.load(os.path.join(self.data[self.working_graph]['location'], self.data[self.working_graph]['gt_file']))
                return
            else:
                super_graph = self.data[self.working_graph]['derived_from']
                self.gt.load(os.path.join(self.data[super_graph]['location'], self.data[super_graph]['gt_file']))
                graph_view = self.create_gt_view(self.edges_location, self.edges[0])
                self.gt = graph_view
        else:
            self.gt.load(os.path.join(self.data[self.working_graph]['location'], self.data[self.working_graph]['gt_file']))

    def create_gt_view(self, path, file):
        prop_map = self.gt.new_edge_property('bool')
        df = self.load_edges(os.path.join(path, file))
        df = self.resolve_ids(df)
        for key, item in df.iterrows():
            prop_map[self.gt.edge(item['source'], item['target'])] = True
        graph_view = GraphView(self.gt, efilt=prop_map)
        graph_view = GraphView(graph_view, vfilt=lambda v: v.out_degree() > 0 or v.in_degree() > 0)
        return graph_view

    def resolve_ids(self, df):
        df = pd.merge(df, self.gt_wiki_id_map, how='inner', left_on='source', right_on='wiki_id')
        df = df[['gt_id', 'target', 'type', 'cscore']]
        df.columns = ['source', 'target', 'type', 'cscore']
        df = pd.merge(df, self.gt_wiki_id_map, how='inner', left_on='target', right_on='wiki_id')
        df = df[['source', 'gt_id', 'type', 'cscore']]
        df.columns = ['source', 'target', 'type', 'cscore']
        return df

    @staticmethod
    def load_drawing_props(file):
        with open(os.path.join(os.getcwd(), file), 'r') as f:
            dprops = json.load(f)
        return dprops

    @staticmethod
    def process_drawing_properties(graph, drawing_props):
        dprops = drawing_props
        g = graph
        vcount = len(list(g.vertices()))
        if vcount == 0:
            dprops['vprops'] = {}
            dprops['eprops'] = {}
            return dprops

        vmin = 8
        vmax = 20
        emin = 1
        emax = 2
        font_size = 8
        dprops['output_width'] = vcount * 10
        dprops['output_height'] = vcount * 10

        if dprops['output_width'] < 600:
            dprops['output_width'] = 600
            dprops['output_height'] = 600
        elif dprops['output_width'] >= 30000:
            dprops['output_width'] = 30000
            dprops['output_height'] = 30000


        '''
        if vcount <= 25:
            dprops['output_width'] = vcount * 50
            dprops['output_height'] = vcount * 50
            vmin = 8
            vmax = 20
            emin = 1
            emax = 2
            font_size = 8
        elif vcount > 25 and (vcount * 10) < 30000:
            dprops['output_width'] = vcount * 20
            dprops['output_height'] = vcount * 20
            vmin = 8
            vmax = 20
            emin = 1
            emax = 2
            font_size = 8
        elif (vcount * 5) >= 30000:
            dprops['output_width'] = 30000
            dprops['output_height'] = 30000
            vmin = 10
            vmax = 100
            emin = 2
            emax = 8
            font_size = 8
        '''
        dprops['vprops']['font_size'] = font_size
        if dprops['vprops']['size'] == 'cscore' and vcount > 0:
            vertex_size = g.new_vertex_property("double")
            graph_tool.map_property_values(g.vp.cscore, vertex_size, lambda x: x + 0.1)
            vertex_size = graph_tool.draw.prop_to_size(vertex_size, mi=vmin, ma=vmax, log=False, power=0.5)
            dprops['vprops']['size'] = vertex_size

        if dprops['vprops']['text'] == 'title':
            label = g.vp.title
            dprops['vprops']['text'] = label

        if dprops['color_by_type']:
            vertex_color = g.new_vertex_property("string")
            graph_tool.map_property_values(g.vp.ns, vertex_color,
                                           lambda x: dprops['cat_color'] if x == '14.0' else
                                           dprops['link_color'])
            dprops['vprops']['fill_color'] = vertex_color
        if dprops['eprops']['pen_width'] == 'cscore':
            edge_size = g.new_edge_property("double")
            graph_tool.map_property_values(g.ep.cscore, edge_size, lambda x: x + 0.1)
            edge_size = graph_tool.draw.prop_to_size(edge_size, mi=emin, ma=emax, log=False, power=0.5)
            dprops['eprops']['pen_width'] = edge_size
        return dprops

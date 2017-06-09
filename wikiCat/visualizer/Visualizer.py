from wikiCat.processors.GtGraphProcessor import GtGraphProcessor
from graph_tool.all import *
import os
import pprint
import pandas as pd


class Visualizer(GtGraphProcessor):
    def __init__(self, graph):
        GtGraphProcessor.__init__(self, graph)
        self.gt = Graph()
        self.data = self.graph.data
        self.working_graph = self.graph.curr_working_graph
        self.results_path = os.path.join(self.project.results_path, self.working_graph)
        self.gt_wiki_id_map_file = os.path.join(self.data['main']['location'], self.data['main']['gt_wiki_id_map'])
        self.gt_wiki_id_map = pd.read_csv(os.path.join(self.data['main']['location'], self.data['main']['gt_wiki_id_map']),
                                          header=None, delimiter='\t', names=['wiki_id', 'gt_id'])
        self.drawing_props = {}
        self.set_drawing_properties(vertex_text=None, vertex_text_color='black', vertex_font_size=24,
                                    vertex_font_family='sans-serif', vertex_color=[1, 1, 1, 0],
                                    vertex_fill_color='black', vertex_size=1, edge_color='black', edge_pen_width=1.2)
        # Print all class variables
        self.pp = pprint.PrettyPrinter(indent=4)
        #v = vars(self)
        #self.pp.pprint(v)

        print('Initialized graph visualizer')

    def snapshots(self, stype, outtype='png', vsize=None, vlabel=None, color_by_type=True, esize=None):
        self.load()
        print('(Sub) Graph loaded')
        self.results_path = os.path.join(self.results_path, stype)
        snapshot_files = self.data[self.working_graph][stype]['files']
        snapshot_path = os.path.join(self.graph.data[self.working_graph]['location'], stype)
        graph_views = self.create_snapshot_views(snapshot_path, snapshot_files)
        for key in graph_views.keys():
            self.visualize(graph_views[key], key, outtype, vsize=vsize, vlabel=vlabel, color_by_type=color_by_type, esize=esize)

    def create_snapshot_views(self, path, files):
        graph_views = {}
        for file in files:
            prop_map = self.gt.new_edge_property('bool')
            df = self.load_edges(os.path.join(path, file))
            if len(df.index) > 0:
                for key, item in df.iterrows():
                    prop_map[self.gt.edge(item['source'], item['target'])] = True
                graph_views[file[:-4]] = GraphView(self.gt, efilt=prop_map)
                graph_views[file[:-4]] = GraphView(graph_views[file[:-4]],
                                                   vfilt=lambda v: v.out_degree() > 0 or v.in_degree() > 0)
        return graph_views

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
            graph_type = 'main'
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

    def set_drawing_properties(self, vertex_text=None, vertex_text_color=None, vertex_font_size=None,
                               vertex_font_family=None, vertex_color=None, vertex_fill_color=None,
                               vertex_size=None, edge_color=None, edge_pen_width=None):

        if 'vprops' in self.drawing_props.keys():
            vprops = self.drawing_props['vprops']
        else:
            vprops = {}
        if 'eprops' in self.drawing_props.keys():
            eprops = self.drawing_props['eprops']
        else:
            eprops = {}
        if vertex_text is not None:
            vprops['text'] = vertex_text
        if vertex_text_color is not None:
            vprops['text_color'] = vertex_text_color
        if vertex_font_size is not None:
            vprops['font_size'] = vertex_font_size
        if vertex_font_family is not None:
            vprops['font_family'] = vertex_font_family
        if vertex_color is not None:
            vprops['color'] = vertex_color
        if vertex_fill_color is not None:
            vprops['fill_color'] = vertex_fill_color
        if vertex_size is not None:
            vprops['size'] = vertex_size
        if edge_color is not None:
            eprops['color'] = edge_color
        if edge_pen_width is not None:
            eprops['pen_width'] = edge_pen_width
        self.drawing_props['vprops'] = vprops
        self.drawing_props['eprops'] = eprops
        return

    def process_drawing_properties(self, graph, vsize=None, vlabel=None, color_by_type=None, esize=None):
        g = graph
        if vsize is not None and type(vsize) == int or float:
            vertex_size = vsize
            self.set_drawing_properties(vertex_size=vertex_size)
        elif vsize == 'cscore':
            vertex_size = g.new_vertex_property("double")
            graph_tool.map_property_values(g.vp.cscore, vertex_size, lambda x: x + 0.1)
            vertex_size = graph_tool.draw.prop_to_size(vertex_size, mi=0.5, ma=1, log=False, power=0.5)
            self.set_drawing_properties(vertex_size=vertex_size)
        if vlabel == 'title':
            label = g.vp.title
            self.set_drawing_properties(vertex_text=label)
        if color_by_type:
            vertex_color = g.new_vertex_property("string")
            graph_tool.map_property_values(g.vp.ns, vertex_color, lambda x: 'steelblue' if x == 14 else 'crimson')
            self.set_drawing_properties(vertex_fill_color=vertex_color)
        if esize == 'cscore':
            edge_size = g.new_edge_property("double")
            graph_tool.map_property_values(g.ep.cscore, edge_size, lambda x: x + 0.1)
            edge_size = graph_tool.draw.prop_to_size(edge_size, mi=0.5, ma=1, log=False, power=0.5)
            self.set_drawing_properties(edge_pen_width=edge_size)

    def visualize(self, graph_view, outfile):
        print('visualize function will be implemented in subclass. please make use of subclasses.')


class SFDP(Visualizer):
    def __init__(self, graph):
        Visualizer.__init__(self, graph)

    def visualize(self, graph_view, outfile, outtype, vsize=None, vlabel=None, color_by_type=True, esize=None):
        print('create Viz')
        g = graph_view

        self.process_drawing_properties(g, vsize=vsize, vlabel=vlabel, color_by_type=color_by_type, esize=esize)
        out = os.path.join(self.results_path, 'sfpd_'+outfile+'.'+outtype)
        os.makedirs(os.path.dirname(out), exist_ok=True)
        pos = sfdp_layout(g)
        graph_draw(g, pos, vprops=self.drawing_props['vprops'], eprops=self.drawing_props['eprops'], output_size=(20000, 20000), output=out)


class ARF(Visualizer):
    def __init__(self, graph):
        Visualizer.__init__(self, graph)

    def visualize(self, graph_view, outfile, outtype, vsize=None, vlabel=None, color_by_type=True, esize=None):
        print('create Viz')
        g = graph_view

        self.process_drawing_properties(g, vsize=vsize, vlabel=vlabel, color_by_type=color_by_type, esize=esize)
        out = os.path.join(self.results_path, 'arf_'+outfile+'.'+outtype)
        os.makedirs(os.path.dirname(out), exist_ok=True)
        pos = arf_layout(g)
        graph_draw(g, pos, vprops=self.drawing_props['vprops'], eprops=self.drawing_props['eprops'], output_size=(20000, 20000), output=out)

class FR(Visualizer):
    def __init__(self, graph):
        Visualizer.__init__(self, graph)

    def visualize(self, graph_view, outfile, outtype, vsize=None, vlabel=None, color_by_type=True, esize=None):
        print('create Viz')
        g = graph_view

        self.process_drawing_properties(g, vsize=vsize, vlabel=vlabel, color_by_type=color_by_type, esize=esize)
        out = os.path.join(self.results_path, 'fr_'+outfile+'.'+outtype)
        os.makedirs(os.path.dirname(out), exist_ok=True)
        pos = fruchterman_reingold_layout(g)
        graph_draw(g, pos, vprops=self.drawing_props['vprops'], eprops=self.drawing_props['eprops'], output_size=(20000, 20000), output=out)
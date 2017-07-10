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
        self.working_graph_path = self.graph.data[self.working_graph]['location']
        self.results_path = os.path.join(self.project.results_path, self.working_graph)
        self.gt_wiki_id_map_path, self.gt_wiki_id_map_file = self.find_gt_wiki_id_map()
        self.gt_wiki_id_map = pd.read_csv(os.path.join(self.gt_wiki_id_map_path, self.gt_wiki_id_map_file),
                                          header=None, delimiter='\t', names=['wiki_id', 'gt_id'])
        self.drawing_props = {}
        self.set_drawing_properties(vertex_text=None,
                                    vertex_text_color='black',
                                    vertex_font_size=200,
                                    vertex_font_family='sans-serif',
                                    vertex_color=[1, 1, 1, 0],
                                    vertex_fill_color='lightgray',
                                    vertex_size=1,
                                    vertex_pen_width=0.8,
                                    edge_color='lightgray',
                                    edge_pen_width=1.2)
        self.output_dimension = 1000


        # Print all class variables
        #self.pp = pprint.PrettyPrinter(indent=4)
        #v = vars(self)
        #self.pp.pprint(v)
        #self.pp.pprint(self.drawing_props)
        print('Initialized graph visualizer')

    def find_gt_wiki_id_map(self):
        if 'gt_wiki_id_map' in self.data[self.working_graph].keys():
            file = self.data[self.working_graph]['gt_wiki_id_map']
            path = self.working_graph_path
        else:
            file = self.data['main']['gt_wiki_id_map']
            path = self.data['main']['location']
        return path, file

    def snapshots(self, stype, outtype='png', vsize=None, vlabel=None, color_by_type=True, esize=None):
        self.load()
        print('(Sub) Graph loaded')
        self.results_path = os.path.join(self.results_path, stype)
        snapshot_files = self.data[self.working_graph][stype]['files']
        snapshot_path = os.path.join(self.graph.data[self.working_graph]['location'], stype)
        for file in snapshot_files:
            graph_view = self.create_snapshot_view(snapshot_path, file, stype)
            self.visualize(graph_view, file[:-4], outtype, vsize=vsize, vlabel=vlabel, color_by_type=color_by_type,
                           esize=esize)

    def create_snapshot_view(self, path, file, stype):
        prop_map = stype + '_' + str(file[:-6])
        if prop_map in self.gt.edge_properties.keys():
            #print('PROP MAP EXISTS')
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

    def set_drawing_properties(self, vertex_text=None, vertex_text_color=None, vertex_font_size=None,
                               vertex_font_family=None, vertex_color=None, vertex_fill_color=None,
                               vertex_size=None, vertex_pen_width=None, edge_color=None, edge_pen_width=None):

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
        if vertex_pen_width is not None:
            vprops['pen_width'] = vertex_pen_width
        if edge_color is not None:
            eprops['color'] = edge_color
        if edge_pen_width is not None:
            eprops['pen_width'] = edge_pen_width
        self.drawing_props['vprops'] = vprops
        self.drawing_props['eprops'] = eprops
        return

    def process_drawing_properties(self, graph, vsize=None, vlabel=None, color_by_type=None, esize=None):
        g = graph
        vcount = len(list(g.vertices()))
        print(vcount)

        # Set outputsize
        if vcount <= 25:
            self.output_dimension = 500
            vmin = 10
            vmax = 20
            emin = 2
            emax = 8
            font_size = 8
        elif (vcount * 5) >= 30000:
            self.output_dimension = 30000
            vmin = 3
            vmax = 10
            emin = 1
            emax = 5
            font_size = 5
        else:
            self.output_dimension = vcount * 20
            vmin = 10
            vmax = 20
            emin = 2
            emax = 8
            font_size = 8

        self.set_drawing_properties(vertex_font_size=font_size)

        if vsize is not None and type(vsize) == int or type(vsize) == float:
            vertex_size = vsize
            self.set_drawing_properties(vertex_size=vertex_size)
        elif vsize == 'cscore':
            vertex_size = g.new_vertex_property("double")
            graph_tool.map_property_values(g.vp.cscore, vertex_size, lambda x: x + 0.1)
            vertex_size = graph_tool.draw.prop_to_size(vertex_size, mi=vmin, ma=vmax, log=False, power=0.5)
            self.set_drawing_properties(vertex_size=vertex_size)
        if vlabel == 'title':
            label = g.vp.title
            self.set_drawing_properties(vertex_text=label)
        if color_by_type:
            vertex_color = g.new_vertex_property("string")
            graph_tool.map_property_values(g.vp.ns, vertex_color,
                                           lambda x: 'lightsteelblue' if x == '14.0' else 'salmon')
            self.set_drawing_properties(vertex_fill_color=vertex_color)
        if esize == 'cscore':
            edge_size = g.new_edge_property("double")
            graph_tool.map_property_values(g.ep.cscore, edge_size, lambda x: x + 0.1)
            edge_size = graph_tool.draw.prop_to_size(edge_size, mi=emin, ma=emax, log=False, power=0.5)
            self.set_drawing_properties(edge_pen_width=edge_size)

    def visualize(self, graph_view, outfile):
        print('visualize function will be implemented in subclass. please make use of subclasses.')


class GraphVizVisualizer(Visualizer):
    def __init__(self, graph):
        Visualizer.__init__(self, graph)
        self.drawing_props = {}
        self.set_drawing_properties(vertex_text=None,
                                    vertex_text_color='#000000',
                                    vertex_font_size=200,
                                    vertex_font_family='sans-serif',
                                    vertex_color='#2e3436',
                                    vertex_size=1,
                                    vertex_pen_width=0.8,
                                    edge_color='#6a6a6a',
                                    edge_pen_width=1.2)

    def set_drawing_properties(self, vertex_text=None, vertex_text_color=None, vertex_font_size=None,
                               vertex_font_family=None, vertex_color=None, vertex_size=None, vertex_pen_width=None,
                               edge_color=None, edge_pen_width=None):

        if 'vprops' in self.drawing_props.keys():
            vprops = self.drawing_props['vprops']
        else:
            vprops = {}
        if 'eprops' in self.drawing_props.keys():
            eprops = self.drawing_props['eprops']
        else:
            eprops = {}
        if vertex_text is not None:
            vprops['label'] = vertex_text
        if vertex_text_color is not None:
            vprops['fontcolor'] = vertex_text_color
        if vertex_font_size is not None:
            vprops['fontsize'] = vertex_font_size
        if vertex_font_family is not None:
            vprops['fontname'] = vertex_font_family
        if vertex_color is not None:
            vprops['color'] = vertex_color
        if vertex_size is not None:
            vprops['size'] = vertex_size
        if vertex_pen_width is not None:
            vprops['penwidth'] = vertex_pen_width
        if edge_color is not None:
            eprops['color'] = edge_color
        if edge_pen_width is not None:
            eprops['penwidth'] = edge_pen_width
        self.drawing_props['vprops'] = vprops
        self.drawing_props['eprops'] = eprops
        return

    def process_drawing_properties(self, graph, vsize=None, vlabel=None, color_by_type=None, esize=None):
        g = graph
        vcount = len(list(g.vertices()))
        print(vcount)

        #Set output size
        if vcount <= 25:
            self.output_dimension = 500
            vmin = 10
            vmax = 20
            emin = 2
            emax = 8
            font_size = 8
        elif (vcount * 5) >= 30000:
            self.output_dimension = 30000
            vmin = 3
            vmax = 10
            emin = 1
            emax = 5
            font_size = 5
        else:
            self.output_dimension = vcount * 20
            vmin = 10
            vmax = 20
            emin = 2
            emax = 8
            font_size = 8

        self.set_drawing_properties(vertex_font_size=font_size)

        if vsize is not None and type(vsize) == int or type(vsize) == float:
            vertex_size = vsize
            self.set_drawing_properties(vertex_size=vertex_size)
            print('SET HERE')
        elif vsize == 'cscore':
            vertex_size = g.new_vertex_property("double")
            graph_tool.map_property_values(g.vp.cscore, vertex_size, lambda x: x + 0.1)
            vertex_size = graph_tool.draw.prop_to_size(vertex_size, mi=vmin, ma=vmax, log=False, power=0.5)
            self.set_drawing_properties(vertex_size=vertex_size)
        if vlabel == 'title':
            label = g.vp.title
            self.set_drawing_properties(vertex_text=label)
        if color_by_type:
            vertex_color = g.new_vertex_property("string")
            graph_tool.map_property_values(g.vp.ns, vertex_color, lambda x: '#2e3436' if x == '14.0' else '#a40000')
            self.set_drawing_properties(vertex_fill_color=vertex_color)
        if esize == 'cscore':
            edge_size = g.new_edge_property("double")
            graph_tool.map_property_values(g.ep.cscore, edge_size, lambda x: x + 0.1)
            edge_size = graph_tool.draw.prop_to_size(edge_size, mi=emin, ma=emax, log=False, power=0.5)
            self.set_drawing_properties(edge_pen_width=edge_size)


class SFDP(Visualizer):
    def __init__(self, graph):
        Visualizer.__init__(self, graph)

    def visualize(self, graph_view, outfile, outtype, vsize=None, vlabel=None, color_by_type=True, esize=None):
        print('create Viz')
        g = graph_view
        try:
            self.process_drawing_properties(g, vsize=vsize, vlabel=vlabel, color_by_type=color_by_type, esize=esize)
            out = os.path.join(self.results_path, 'sfpd_'+outfile+'.'+outtype)
            os.makedirs(os.path.dirname(out), exist_ok=True)
            pos = sfdp_layout(g)
            graph_draw(g, pos, vprops=self.drawing_props['vprops'], eprops=self.drawing_props['eprops'],
                       vertex_text_position=-2, output_size=(self.output_dimension, self.output_dimension), output=out)
        except Exception as e:
            print(e)


class ARF(Visualizer):
    def __init__(self, graph):
        Visualizer.__init__(self, graph)

    def visualize(self, graph_view, outfile, outtype, vsize=None, vlabel=None, color_by_type=True, esize=None):
        print('Start creating Viz')
        g = graph_view
        try:
            self.process_drawing_properties(g, vsize=vsize, vlabel=vlabel, color_by_type=color_by_type, esize=esize)
            out = os.path.join(self.results_path, 'arf_'+outfile+'.'+outtype)
            os.makedirs(os.path.dirname(out), exist_ok=True)
            pos = arf_layout(g, max_iter=100, dt=1e-4) #According to https://git.skewed.de/count0/graph-tool/issues/239 setting max_iter and dt fixed cairo error. Check quality?!
            graph_draw(g, pos, vprops=self.drawing_props['vprops'], eprops=self.drawing_props['eprops'],
                       vertex_text_position=-2, output_size=(self.output_dimension, self.output_dimension),
                       fit_view=0.95, output=out) #fit_view=(self.output_dimension/2, self.output_dimension/2, self.output_dimension/2, self.output_dimension/2),
            print('Viz created: ' + outfile)
        except Exception as e:
            print(e)


class FR(Visualizer):
    def __init__(self, graph):
        Visualizer.__init__(self, graph)

    def visualize(self, graph_view, outfile, outtype, vsize=None, vlabel=None, color_by_type=True, esize=None):
        print('create Viz')
        g = graph_view
        try:
            self.process_drawing_properties(g, vsize=vsize, vlabel=vlabel, color_by_type=color_by_type, esize=esize)
            out = os.path.join(self.results_path, 'fr_'+outfile+'.'+outtype)
            os.makedirs(os.path.dirname(out), exist_ok=True)
            pos = fruchterman_reingold_layout(g)
            graph_draw(g, pos, vprops=self.drawing_props['vprops'], eprops=self.drawing_props['eprops'], vertex_text_position=-2, output_size=(self.output_dimension, self.output_dimension), output=out)
        except Exception as e:
            print(e)


class RTL(Visualizer):
    def __init__(self, graph):
        Visualizer.__init__(self, graph)

    def visualize(self, graph_view, seed, outfile, outtype, vsize=None, vlabel=None, color_by_type=True, esize=None):
        #print('Create Viz')
        g = graph_view
        try:
            self.process_drawing_properties(g, vsize=vsize, vlabel=vlabel, color_by_type=color_by_type, esize=esize)
            out = os.path.join(self.results_path, 'RTL_'+outfile+'.'+outtype)
            os.makedirs(os.path.dirname(out), exist_ok=True)
            pos = radial_tree_layout(g, seed[0])
            graph_draw(g, pos, vprops=self.drawing_props['vprops'], eprops=self.drawing_props['eprops'],
                       vertex_text_position=-2, output_size=(self.output_dimension, self.output_dimension), output=out)
        except Exception as e:
            print(e)

    def snapshots(self, stype, seed=None, outtype='png', vsize=None, vlabel=None, color_by_type=True, esize=None):
        self.load()
        print('(Sub) Graph loaded')
        seed = graph_tool.util.find_vertex(self.gt, self.gt.vp.id, str(seed))
        self.results_path = os.path.join(self.results_path, stype)
        snapshot_files = self.data[self.working_graph][stype]['files']
        snapshot_path = os.path.join(self.graph.data[self.working_graph]['location'], stype)

        for file in snapshot_files:
            graph_view = self.create_snapshot_view(snapshot_path, file, stype)
            self.visualize(graph_view, seed, file[:-4], outtype, vsize=vsize, vlabel=vlabel, color_by_type=color_by_type,
                           esize=esize)

        #graph_views = self.create_snapshot_view(snapshot_path, snapshot_files, stype)
        #for key in graph_views.keys():
        #    self.visualize(graph_views[key], seed, key, outtype, vsize=vsize, vlabel=vlabel, color_by_type=color_by_type, esize=esize)


class DOT(GraphVizVisualizer):
    def __init__(self, graph):
        GraphVizVisualizer.__init__(self, graph)

    def visualize(self, graph_view, outfile, outtype, vsize=None, vlabel=None, color_by_type=True, esize=None):
        print('create Viz')
        g = graph_view
        try:
            self.process_drawing_properties(g, vsize=vsize, vlabel=vlabel, color_by_type=color_by_type, esize=esize, graphviz=True)
            vprops = {}
            vprops['label'] = self.drawing_props['vprops']['text']
            out = os.path.join(self.results_path, 'dot_'+outfile+'.'+outtype)
            os.makedirs(os.path.dirname(out), exist_ok=True)
            pos = sfdp_layout(g)
            graphviz_draw(g, size=(self.output_dimension, self.output_dimension), vcolor=self.drawing_props['vprops']['fill_color'], vprops=vprops, layout='dot', ratio='auto', output=out) #vprops=self.drawing_props['vprops'], eprops=self.drawing_props['eprops']
            #graph_draw(g, pos, vprops=self.drawing_props['vprops'], eprops=self.drawing_props['eprops'], vertex_text_position=-2, output_size=(self.output_dimension, self.output_dimension), output=out)
            print('Done')
        except Exception as e:
            print(e)
        try:
            print(self.drawing_props['vprops']['text'])

            for i in self.drawing_props['vprops']['text']:
                print(i)
        except:
            pass
        #for i in self.drawing_props['vprops']['text']:
        #    print(i)

            #vcolor=self.drawing_props['vprops']['fill_color']
            ##2e3436
from wikiCat.visualizer.Visualizer import Visualizer
from graph_tool.all import *
import sys, os
from gi.repository import Gtk, Gdk, GdkPixbuf, GObject
import pandas as pd
import math
import cairo


class DynVisualizer(Visualizer):
    def __init__(self, graph):
        Visualizer.__init__(self, graph)
        self.events_file = os.path.join(self.working_graph_path, self.data[self.working_graph]['source_events'][0])
        self.events = self.resolve_ids(pd.read_csv(self.events_file, header=None, delimiter='\t',
                                                   names=['time', 'source', 'target', 'event', 'cscore']))
        self.events = self.events.sort_values('time')
        self.load()
        self.cscore = self.gt.new_edge_property('double')
        self.count = 0
        self.last_update = self.gt.new_edge_property('double')
        self.decay_rate = 0.00000001
        self.curr_time = None
        self.graph_generator = None
        self.output_size = (500, 500)
        self.set_drawing_properties(vertex_size=10,
                                    vertex_min=None,
                                    vertex_max=None,
                                    vertex_text=True,
                                    vertex_font_size=5,
                                    vertex_color_by_type=True,
                                    edge_size='cscore')

    def resolve_ids(self, df):
        df = pd.merge(df, self.gt_wiki_id_map, how='inner', left_on='source', right_on='wiki_id')
        df = df[['time', 'gt_id', 'target', 'event', 'cscore']]
        df.columns = [['time', 'source', 'target', 'event', 'cscore']]
        df = pd.merge(df, self.gt_wiki_id_map, how='inner', left_on='target', right_on='wiki_id')
        df = df[['time', 'source', 'gt_id', 'event', 'cscore']]
        df.columns = [['time', 'source', 'target', 'event', 'cscore']]
        return df

    def dynamic_graph_generator(self):
        for e in self.events.iterrows():
            self.curr_time = e[1]['time']
            if e[1]['event'] == 'start':
                tmp = self.gt.add_edge(e[1]['source'], e[1]['target'])
                self.cscore[tmp] = e[1]['cscore']
                self.last_update[tmp] = e[1]['time']
                yield  self.gt
            elif e[1]['event'] == 'end':
                tmp = self.gt.edge(e[1]['source'], e[1]['target'])
                self.cscore[tmp] = e[1]['cscore']
                self.gt.remove_edge(tmp)
                self.last_update[tmp] = e[1]['time']
                yield self.gt

    def update_state(self):
        pass

    def update_graph(self, g, exists):
        # Machen die Updates zu CSCORE SINN???????? ODER MAche ich hier Etwas MASsiv FaLsch?
        for edge in g.edges():
            delta = self.curr_time - self.last_update[edge]
            if self.cscore[edge] * math.exp(-1 * self.decay_rate * delta) < 1:
                self.cscore[edge] = 1
            else:
                self.cscore[edge] = self.cscore[edge] * math.exp(-1 * self.decay_rate * delta)
            exists[edge.source()] = True
            exists[edge.target()] = True
        return g, exists

    def set_drawing_properties(self, output_size=None, vertex_size=None, vertex_min=None, vertex_max=None,
                               vertex_text=None, vertex_font_size=None, vertex_color_by_type=None, edge_size=None,
                               edge_min=None, edge_max=None):

        #Missing vertex_text_color, vertext font family
        #vertex_fill_color is not None: vprops['fill_color'] = vertex_fill_color, if vertex_pen_width is not None:
        #    vprops['pen_width'] = vertex_pen_width
        #if edge_color is not None:
        #    eprops['color'] = edge_color

        if output_size is not None and type(output_size) == tuple:
            self.output_size = output_size
        elif output_size is not None and type(output_size) == int:
            self.output_size = (output_size, output_size)

        if 'vprops' in self.drawing_props.keys():
            vprops = self.drawing_props['vprops']
        else:
            vprops = {}

        if 'eprops' in self.drawing_props.keys():
            eprops = self.drawing_props['eprops']
        else:
            eprops = {}

        if vertex_size is not None and type(vertex_size) == int or type(vertex_size) == float:
            vprops['size'] = vertex_size
        elif vertex_size == 'cscore':
            vertex_size = self.gt.new_vertex_property("double")
            graph_tool.map_property_values(self.gt.vp.cscore, vertex_size, lambda x: x + 0.1)
            if vertex_min is not None and vertex_max is not None and vertex_min < vertex_max:
                vertex_size = graph_tool.draw.prop_to_size(vertex_size, mi=vertex_min, ma=vertex_max, log=False, power=0.5)
            else:
                vertex_size = graph_tool.draw.prop_to_size(vertex_size, mi=10, ma=100, log=False, power=0.5)
            vprops['size'] = vertex_size

        if vertex_text:
            vprops['text'] = self.gt.vp.title

        if vertex_font_size is not None:
            vprops['font_size'] = vertex_font_size

        if vertex_color_by_type:
            vertex_color = self.gt.new_vertex_property("string")
            graph_tool.map_property_values(self.gt.vp.ns, vertex_color, lambda x: '#f9e3da' if x == '14.0' else '#b4bfc5')
            vprops['color'] = vertex_color

        if edge_size == 'cscore':
            edge_size = self.gt.new_edge_property("double")
            graph_tool.map_property_values(self.gt.ep.cscore, edge_size, lambda x: x + 0.1)
            if edge_min is not None and edge_max is not None and edge_min < edge_max:
                edge_size = graph_tool.draw.prop_to_size(edge_size, mi=edge_min, ma=edge_max, log=False, power=0.5)
            else:
                edge_size = graph_tool.draw.prop_to_size(edge_size, mi=10, ma=100, log=False, power=0.5)
            eprops['pen_width'] = edge_size
        self.drawing_props['vprops'] = vprops
        self.drawing_props['eprops'] = eprops
        return


class ARFVid(DynVisualizer):
    def __init__(self, graph):
        DynVisualizer.__init__(self, graph)
        self.pos = None
        self.win = None

    def update_state(self):
        g = self.graph_generator.__next__()
        exists = g.new_vertex_property('bool', False)
        reset = g.new_vertex_property('bool', True)

        g, exists = self.update_graph(g, exists)
        g.set_vertex_filter(exists)
        arf_layout(g, pos=self.pos, max_iter=100, dt=1e-4)

        # The movement of the vertices may cause them to leave the display area. The
        # following function rescales the layout to fit the window to avoid this.
        #if count > 0 and count % 1000 == 0:
        #    win.graph.fit_to_window(ink=True)

        # The following will force the re-drawing of the graph, and issue a
        # re-drawing of the GTK window.
        self.win.graph.regenerate_surface()
        self.win.graph.queue_draw()

        g.set_vertex_filter(reset)

        # We need to return True so that the main loop will call this function more
        # than once.
        self.count += 1
        return True

    def visualize(self, output_size=None, vertex_size=None, vertex_min=None, vertex_max=None, vertex_text=None,
                  vertex_font_size=None, vertex_color_by_type=True, edge_size=None, edge_min=None, edge_max=None):

        self.set_drawing_properties(output_size=output_size,
                                    vertex_size=vertex_size,
                                    vertex_min=vertex_min,
                                    vertex_max=vertex_max,
                                    vertex_text=vertex_text,
                                    vertex_font_size=vertex_font_size,
                                    vertex_color_by_type=vertex_color_by_type,
                                    edge_size=edge_size,
                                    edge_min=edge_min,
                                    edge_max=edge_max)
        self.pos = arf_layout(self.gt, max_iter=100, dt=1e-4)
        self.graph_generator = self.dynamic_graph_generator()
        self.gt.clear_edges()
        self.win = GraphWindow(self.gt,
                               self.pos,
                               geometry=self.output_size,
                               vprops=self.drawing_props['vprops'],
                               eprops=self.drawing_props['eprops'],
                               edge_marker_size=5,  # müsste auch noch über set properties gesetzt werden können.
                               vertex_text_position=-2,  # müsste auch noch über set properties gesetzt werden können.
                               vertex_font_family='sans-serif',  # müsste auch noch über set properties gesetzt werden können.
                               vertex_font_weight=cairo.FONT_WEIGHT_BOLD, # müsste auch noch über set properties gesetzt werden können.
                               edge_color='#cbcbcb')  # müsste auch noch über set properties gesetzt werden können.

        cid = GObject.idle_add(self.update_state)

        # We will give the user the ability to stop the program by closing the window.
        self.win.connect("delete_event", Gtk.main_quit)

        # Actually show the window, and start the main loop.
        self.win.show_all()
        Gtk.main()

    def test(self):
        self.graph_generator = self.dynamic_graph_generator()

        for i in self.graph_generator:
            print('HIER')
        #    print(self.graph_generator.__next__())
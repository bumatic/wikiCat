import cairo
from graph_tool.draw import arf_layout, GraphWindow

from wikiCat.visualizer.dyn_visualizer import DynVisualizer


class ARFVid(DynVisualizer):
    def __init__(self, graph):
        DynVisualizer.__init__(self, graph)
        self.pos = None
        self.win = None

    def update_state(self):
        print('davor')
        g = self.graph_generator.__next__()
        #print(g)
        exists = g.new_vertex_property('bool', False)
        reset = g.new_vertex_property('bool', True)

        g, exists = self.update_graph(g, exists)
        g.set_vertex_filter(exists)
        self.set_drawing_properties(edge_size='cscore')
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
            pass
            #print('HIER')
            #print(self.graph_generator.__next__())
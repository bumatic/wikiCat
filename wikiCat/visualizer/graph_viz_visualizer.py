import graph_tool

from wikiCat.visualizer.static_visualizer import StaticVisualizer


class GraphVizVisualizer(StaticVisualizer):
    def __init__(self, graph):
        StaticVisualizer.__init__(self, graph)
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
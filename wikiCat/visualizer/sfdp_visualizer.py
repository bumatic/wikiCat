import os
from graph_tool.draw import sfdp_layout, graph_draw
from wikiCat.visualizer.static_visualizer import StaticVisualizer


class SFDP(StaticVisualizer):
    def __init__(self, graph):
        StaticVisualizer.__init__(self, graph)

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
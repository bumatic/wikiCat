import os

from graph_tool.draw import arf_layout, graph_draw

from wikiCat.visualizer.static_visualizer import StaticVisualizer


class ARF(StaticVisualizer):
    def __init__(self, graph):
        StaticVisualizer.__init__(self, graph)

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
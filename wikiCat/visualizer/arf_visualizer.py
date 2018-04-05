import os
from graph_tool.draw import arf_layout, graph_draw
from wikiCat.visualizer.static_visualizer import StaticVisualizer


class ARF(StaticVisualizer):
    def __init__(self, graph):
        StaticVisualizer.__init__(self, graph)

    def visualize(self, graph_view, outfile, drawing_props):
        g = graph_view
        props = self.process_drawing_properties(g, drawing_props)
        out = os.path.join(self.results_path, 'arf_' + drawing_props['props_type'] + '_' + outfile + '.' + props['fmt'])
        os.makedirs(os.path.dirname(out), exist_ok=True)

        pos = arf_layout(g,max_iter=0)
        # pos = arf_layout(g, max_iter=100, dt=1e-4)
        # According to https://git.skewed.de/count0/graph-tool/issues/239 setting max_iter and dt fixed cairo error.
        # Check quality?!
        try:
            if len(list(g.vertices())) > 0:
                graph_draw(g, pos, vprops=props['vprops'], eprops=props['eprops'], output_size=(props['output_width'], props['output_height']), output=out)
        except Exception as e:
            print(e)

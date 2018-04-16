import os

from graph_tool.draw import fruchterman_reingold_layout, graph_draw

from wikiCat.visualizer.static_visualizer import StaticVisualizer


class FR(StaticVisualizer):
    def __init__(self, project):
        StaticVisualizer.__init__(self, project)

    def visualize(self, graph_view, outfile, drawing_props):
        g = graph_view
        props = self.process_drawing_properties(g, drawing_props)
        out = os.path.join(self.results_path, 'fr_' + drawing_props['props_type'] + '_' + outfile + '.' + props['fmt'])
        os.makedirs(os.path.dirname(out), exist_ok=True)

        try:
            if len(list(g.vertices())) > 0:
                # TODO: LAYOUT IST SCHEISSE. ÄNDERE DEFAULT!
                pos = fruchterman_reingold_layout(g)
                graph_draw(g, pos, vprops=props['vprops'], eprops=props['eprops'],
                           output_size=(props['output_width'], props['output_height']), output=out)
        except Exception as e:
            print(e)


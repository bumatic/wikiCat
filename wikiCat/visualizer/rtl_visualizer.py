import os

import graph_tool
from graph_tool.draw import radial_tree_layout, graph_draw

from wikiCat.visualizer.static_visualizer import StaticVisualizer


class RTL(StaticVisualizer):
    def __init__(self, graph):
        StaticVisualizer.__init__(self, graph)

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
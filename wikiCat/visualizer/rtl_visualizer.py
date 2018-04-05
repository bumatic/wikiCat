import os

import graph_tool
from graph_tool.draw import radial_tree_layout, graph_draw

from wikiCat.visualizer.static_visualizer import StaticVisualizer


class RTL(StaticVisualizer):
    def __init__(self, graph):
        StaticVisualizer.__init__(self, graph)

    def visualize(self, graph_view, seed, outfile, drawing_props):
        g = graph_view
        props = self.process_drawing_properties(g, drawing_props)
        out = os.path.join(self.results_path, 'rtl_' + drawing_props['props_type'] + '_' + outfile + '.' + props['fmt'])
        os.makedirs(os.path.dirname(out), exist_ok=True)
        print(self.graph)
        #print(seed)
        #print(type(seed))
        # TODO DOES NOT WORK PROPERLY. USE ONLY FOR SUBCATS AND RESOLVE SEED ID WITH WIKI ID MAP!!!!

        pos = radial_tree_layout(g, seed[0])
        try:
            if len(list(g.vertices())) > 0:
                graph_draw(g, pos, vprops=props['vprops'], eprops=props['eprops'],
                           output_size=(props['output_width'], props['output_height']), output=out)
        except Exception as e:
            print(e)

    '''
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
    '''

    def snapshots(self, stype, drawing_props_file=None, seed=None):
        self.load()
        seed = graph_tool.util.find_vertex(self.gt, self.gt.vp.id, str(seed))
        self.results_path = os.path.join(self.results_path, stype)
        snapshot_files = self.data[self.working_graph][stype]['files']
        snapshot_path = os.path.join(self.graph.data[self.working_graph]['location'], stype)
        for file in snapshot_files:
            if drawing_props_file is not None:
                drawing_props = self.load_drawing_props(os.path.join(self.project.pinfo['path']['gt_props'], drawing_props_file))
            else:
                drawing_props = self.load_drawing_props(os.path.join('wikiCat', 'visualizer', 'drawing_default.json'))
            graph_view = self.create_snapshot_view(snapshot_path, file, stype)
            self.visualize(graph_view, seed, file[:-4], drawing_props)
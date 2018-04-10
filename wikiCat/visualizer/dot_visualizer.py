import os

from graph_tool.draw import sfdp_layout, graphviz_draw

from wikiCat.visualizer.graph_viz_visualizer import GraphVizVisualizer


class DOT(GraphVizVisualizer):
    def __init__(self, graph):
        GraphVizVisualizer.__init__(self, graph)

    def visualize(self, graph_view, outfile, drawing_props):
        print('Is not yet implemented correctly')
        '''
        g = graph_view
        props = self.process_drawing_properties(g, drawing_props)
        out = os.path.join(self.results_path, 'dot_' + drawing_props['props_type'] + '_' + outfile + '.' + props['fmt'])
        os.makedirs(os.path.dirname(out), exist_ok=True)
        pos = sfdp_layout(g)
        try:
            
        except Exception as e:
            print(e)
        '''

    '''
    # OLD IMPLEMENTATION WHICH PROBABLY WAS NOT CORRECT:
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
    '''
from wikiCat.selectors.selector import *
from wikiCat.wikiproject import Project
from wikiCat.processors.GtGraphProcessor import *
from wikiCat.visualizer.Visualizer import *
from wikiCat.visualizer.DynVisualizer import *


mp = Project('project')
mp.load_project()
g = mp.gt_graph_objs['fixed_none__errors_removed']

g.set_working_graph('Bitcoin_2sub_gt')
ARFVid(g).visualize(output_size=(1000,800), vertex_size=10, vertex_min=None, vertex_max=None, vertex_text=True,
              vertex_font_size=None, vertex_color_by_type=True, edge_size='cscore', edge_min=1, edge_max=10)
#ARFVid(g).test()

#SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')




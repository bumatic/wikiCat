from wikiCat.selector.selector import *
from wikiCat.visualizer.arf_vid_visualizer import ARFVid
from wikiCat.wikiproject import Project
from wikiCat.processor.GtGraphProcessor import *
from wikiCat.visualizer.visualizer import *
from wikiCat.visualizer.dyn_visualizer import *


mp = Project('project')
mp.load_project()
g = mp.gt_graph_objs['fixed_none__errors_removed']

g.set_working_graph('Cryptocurrency_2sub_gt')
ARFVid(g).visualize(output_size=(1500,1500), vertex_size='cscore', vertex_min=10, vertex_max=30, vertex_text=True,
              vertex_font_size=None, vertex_color_by_type=True, edge_size='cscore', edge_min=3, edge_max=30)

#ARFVid(g).test()
#SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')


from wikiCat.selectors.selector import *
from wikiCat.wikiproject import Project
from wikiCat.processors.GtGraphProcessor import *
from wikiCat.visualizer.Visualizer import *
from wikiCat.visualizer.DynVisualizer import *


mp = Project('project')
mp.load_project()
g = mp.gt_graph_objs['fixed_none__errors_removed']

g.set_working_graph('Geosocial_networking_3_sub_1_super_gt')
ARFVid(g).visualize()
#ARFVid(g).test()

#SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
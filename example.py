from wikiCat.processor.parsed_data_error_check import ParsedDataErrorCheck
from wikiCat.processor.graph_data_gererator import GraphDataGenerator
from wikiCat.processor.spark_processor_parsed import SparkProcessorParsed
from wikiCat.processor.controvercy_score import ControvercyScore
from wikiCat.processor.oldest_revision import OldestRevision
from wikiCat.processor.gt_graph_generator import GtGraphGenerator
from wikiCat.selector.selector import Selector
from wikiCat.data.wikigraph import WikiGraph
from wikiCat.selector.selector import *
from wikiCat.visualizer.arf_vid_visualizer import ARFVid
from wikiCat.wikiproject import Project
from wikiCat.processor.GtGraphProcessor import *
from wikiCat.visualizer.visualizer import *
from wikiCat.visualizer.dyn_visualizer import *


mp = Project('project')
mp.load_project()
g = mp.gt_graph_objs['fixed_none__errors_removed']

g.set_working_graph('main')
#SubGraph(g).create(title='Simulation_1_sub_1_super_gt', seed=[7744777], cats=True,
#                   subcats=1, supercats=1)
g.set_working_graph('Simulation_1_sub_1_super_gt')
#SubGraphProcessor(g).create_gt_subgraph()
#Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
#SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
#ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')

ARFVid(g).visualize(output_size=(1500,1500), vertex_size='cscore', vertex_min=10, vertex_max=30, vertex_text=True,
              vertex_font_size=None, vertex_color_by_type=True, edge_size='cscore', edge_min=1, edge_max=10)

#ARFVid(g).visualize(output_size=(500,500), vertex_size=10, vertex_min=None, vertex_max=None, vertex_text=True,
#              vertex_font_size=None, vertex_color_by_type=True, edge_size='cscore', edge_min=1, edge_max=10)

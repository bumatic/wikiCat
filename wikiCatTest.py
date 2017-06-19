import pprint
from wikiCat.processors.parsed_data_error_check import ParsedDataErrorCheck
from wikiCat.processors.graph_data_gererator import GraphDataGenerator
from wikiCat.processors.spark_processor_parsed import SparkProcessorParsed
from wikiCat.processors.controvercy_score import ControvercyScore
from wikiCat.processors.oldest_revision import OldestRevision
from wikiCat.processors.gt_graph_generator import GtGraphGenerator
from wikiCat.selectors.selector import Selector
from wikiCat.data.wikigraph import WikiGraph
from wikiCat.selectors.selector import *
from wikiCat.wikiproject import Project
from wikiCat.processors.GtGraphProcessor import *
from wikiCat.visualizer.Visualizer import *


mp = Project('project')
mp.load_project()

g = mp.gt_graph_objs['fixed_none__errors_removed']
g.set_working_graph('main')
#SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
#SFDP(g).snapshots('snapshot_year', vsize='cscore')
#ARF(g).snapshots('snapshot_year', vsize='cscore') #, vlabel='title'


'''
g.set_working_graph('main')
SubGraph(g).create(title='ethereum_article_super_3_gt_new', seed=[41754003], cats=True,
                   subcats=0, supercats=3)
g.set_working_graph('ethereum_article_super_3_gt_new')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
'''
g.set_working_graph('ethereum_article_super_3_gt_new')
#g.set_working_graph('bitcoin_subcats_3_gt')
#g.set_working_graph('main')
#SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
DOT(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
#ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')

'''


g.set_working_graph('bitcoin_supercats_3_gt')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
FR(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')


g.set_working_graph('bitcoin_supercats_4_gt')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
FR(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')


g.set_working_graph('bitcoin_supercats_2_gt')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
FR(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
'''

#g.set_working_graph('bitcoin_subcats_2_gt')
#SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
#ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
#FR(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')

'''
g.set_working_graph('bitcoin_subcats_3_gt')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
FR(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
'''
'''
g.set_working_graph('bitcoin_supercats_5_gt')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
FR(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')

g.set_working_graph('bitcoin_supercats_10_gt')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
FR(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
'''

'''
g.set_working_graph('big_data_subcats')
#RTL(g).snapshots('snapshot_year', seed=40423498, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
FR(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')


g.set_working_graph('main')
SubGraph(g).create(title='bitcoin_gt', seed=[39563179], cats=True,
                   subcats=5, supercats=5)
g.set_working_graph('bitcoin_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
'''





'''
#SubGraph(g).create(title='bitcoin_subcats_gt', seed=[39563179], cats=True,
#                   subcats=5, supercats=0)
#g.set_working_graph('bitcoin_subcats_gt')



#SubGraphProcessor(g).create_gt_subgraph()


#Snapshots(g).create('snapshot_year')
#SubGraphProcessor(g).internalize_snapshots('snapshot_year')

#g.set_working_graph('main')
#SubGraph(g).create('bitcoin_supercats_gt', 'snapshot_year',  seed=[39563179], cats=True,
#                   subcats=0, supercats=5)



#ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')

#Snapshots(g).create('snapshot_month', slice='month')

#sgp = SubGraphProcessor(g)
#sgp.create_gt_subgraph()
#sgp.internalize_snapshots('snapshot_year')




g.set_working_graph('big_data_subcats')
#RTL(g).snapshots('snapshot_year', seed=40423498, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
FR(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
'''

#g.set_working_graph('bitcoin')
#SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
#ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
#FR(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')

'''ABOVE SFDP PRDUCES AND ERROR - CHECK

nitialized graph visualizer
(Sub) Graph loaded
create Viz
214
<PropertyMap object with key type 'Vertex' and value type 'double', for Graph 0x1268a8a20, at 0x103203c50>
create Viz
370
<PropertyMap object with key type 'Vertex' and value type 'double', for Graph 0x1268a86d8, at 0x103164240>
Traceback (most recent call last):
  File "wikiCatTest.py", line 26, in <module>
    SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
  File "/Users/ga58sis/Desktop/wikiCat/wikiCat/visualizer/Visualizer.py", line 46, in snapshots
    self.visualize(graph_views[key], key, outtype, vsize=vsize, vlabel=vlabel, color_by_type=color_by_type, esize=esize)
  File "/Users/ga58sis/Desktop/wikiCat/wikiCat/visualizer/Visualizer.py", line 186, in visualize
    graph_draw(g, pos, vprops=self.drawing_props['vprops'], eprops=self.drawing_props['eprops'], vertex_text_position=-2, output_size=self.output_size, output=out)
  File "/usr/local/lib/python3.6/site-packages/graph_tool/draw/cairo_draw.py", line 1130, in graph_draw
    output_size[1])
SystemError: <class 'cairo.ImageSurface'> returned NULL without setting an error

'''

'''
g.set_working_graph('big_data_supercats')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
FR(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')

g.set_working_graph('big_data_subcats')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
FR(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')

g.set_working_graph('prediction_subcats')
#RTL(g).snapshots('snapshot_year', seed=1436435, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
FR(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')

g.set_working_graph('prediction_supercats')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
FR(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')

g.set_working_graph('omics_subcats')
#RTL(g).snapshots('snapshot_year', seed=13693058, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
FR(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')

g.set_working_graph('omics_supercats')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
FR(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')
'''

#g.set_working_graph('main')
#RTL(g).snapshots('snapshot_year', seed=40423498, vsize='cscore', vlabel='title', esize='cscore')
#ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
#SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
#FR(g).snapshots('snapshot_year', vsize='cscore', vlabel='title')




#g.set_working_graph('main')
#SubGraphViews(g).create('big_data_subcats', 'snapshot_year',  seed=[40423498], cats=True,
#                        subcats=5, supercats=0)

#g.set_working_graph('main')
#SubGraphViews(g).create('big_data_supercats', 'snapshot_year',  seed=[40423498], cats=True,
#                        subcats=0, supercats=5)



pp = pprint.PrettyPrinter(indent=4)

#g = mp.gt_graph_objs['fixed_none__errors_removed']
#g.set_working_graph('main')
#SubGraphViews(g).create('bitcoin_subcats', 'snapshot_year',  seed=[39563179], cats=True,
#                        subcats=5, supercats=0)

#SubGraphViews(g).create('bitcoin_supercats', 'snapshot_year',  seed=[39563179], cats=True,
#                        subcats=0, supercats=5)







#GtEvents(g).create('gt_events_all')
#Snapshots(g).create('snapshot_year'
## from wikiCat.selectors.SelectorCscore import SelectorCscore
#SelectorCscore(g).get_highest_cscores('nodes', cats_only=True, n=10)
#mp.find_start_date()
#mp.set_dump_date('2016-11-01')
#mp.find_start_date()
#mp.set_dump_date('2016-11-01')
#pp.pprint(mp.pinfo)
#WikiGraph(mp).
#GtGraphGenerator(mp).register_gt_graph()

#GENERATE SUBGRAPH AND VIEW
#SubGraphViews(g).create('bitcoin_subcats', 'snapshot_year',  seed=[39563179], cats=True,
#                        subcats=10, supercats=0)

#SubGraph(g).create(title='bitcoin', seed=[40261770, 39563179, 42031444, 42132341], subcats=3, supercats=3)





#Selector(g).create_snapshot_views_spark()

#Selector(mp).temporal_views_spark()

#ParsedDataErrorCheck(mp, 'cat_data').missing_info_source_ids()
#ParsedDataErrorCheck(mp, 'cat_data').find_unresolvable_target_titles()

#mp.find_oldest_revision()



#GraphDataGenerator(mp, 'cat_data').generate_graph_data(override=True)
#Generate graph
#GtGraphGenerator(mp).create_gt_graph()
#ControvercyScore(mp).calculate_edge_score()
#ControvercyScore(mp).calculate_avg_node_score()
#ControvercyScore(mp).calculate_avg_edge_score()

#GtGraphGenerator(mp).add_gt_graph()



#cat_data_fixed_none_errors_removed_1_edge

#cs = ControvercyScore(mp)
#cs.calculate_edge_score()
#cs.caculate_node_score()


#pp.pprint(mp.pinfo)


'''
# CODE FOR PROJECT CREATION:
mp = Project('project')
mp.create_project(title='MyWikiCatDevelopmentProject', description='This is my test project for WikiCatDevelopment')
'''

'''
# CODE FOR LOADING PROJECT:
mp = Project('project')
mp.load_project()
'''

'''
# CODE FOR ADDING PARSED DATA TO PROJECT:
mp.add_data('parsed', page_info=['enwiki-20161101_page_info.csv'], revision_info=['enwiki-20161101_revisions.csv'], cat_data=['enwiki-20161101_cats.csv'])
'''


from wikiCat.processor.GtGraphProcessor import *
from wikiCat.processor.GtGraphProcessor import *

from wikiCat.selector.selector_snapshots import Snapshots
from wikiCat.selector.selector_sub_graph import SubGraph
from wikiCat.visualizer.arf_visualizer import ARF
from wikiCat.visualizer.sfdp_visualizer import SFDP
from wikiCat.wikiproject import Project

mp = Project('project')
mp.load_project()
g = mp.gt_graph_objs['fixed_none__errors_removed']

'''
g.set_working_graph('main')
SubGraph(g).create(title='Big_Data_3_sub_1_super_gt', seed=[40423498], cats=True,
                   subcats=3, supercats=1)

g.set_working_graph('Big_Data_3_sub_1_super_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
#RTL(g).snapshots('snapshot_year', seed=22313095, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')

g.set_working_graph('main')
SubGraph(g).create(title='Data_Mining_3_sub_1_super_gt', seed=[5206601], cats=True,
                   subcats=3, supercats=1)

g.set_working_graph('Data_Mining_3_sub_1_super_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
#RTL(g).snapshots('snapshot_year', seed=22313095, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')


g.set_working_graph('main')
SubGraph(g).create(title='Machine_Learning_3_sub_1_super_gt', seed=[706543], cats=True,
                   subcats=3, supercats=1)

g.set_working_graph('Machine_Learning_3_sub_1_super_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
#RTL(g).snapshots('snapshot_year', seed=22313095, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')


g.set_working_graph('main')
SubGraph(g).create(title='AI_3_sub_1_super_gt', seed=[700355], cats=True,
                   subcats=3, supercats=1)

g.set_working_graph('AI_3_sub_1_super_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
#RTL(g).snapshots('snapshot_year', seed=22313095, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
'''

'''
g.set_working_graph('main')
SubGraph(g).create(title='Algorithms_3_sub_gt', seed=[691136], cats=True,
                   subcats=3, supercats=0)
g.set_working_graph('Algorithms_3_sub_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
#RTL(g).snapshots('snapshot_year', seed=22313095, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')


g.set_working_graph('main')
SubGraph(g).create(title='Algorithms_2_sub_gt', seed=[691136], cats=True,
                   subcats=2, supercats=0)
g.set_working_graph('Algorithms_2_sub_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
#RTL(g).snapshots('snapshot_year', seed=22313095, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')

g.set_working_graph('main')
SubGraph(g).create(title='Algorithms_1_super_1_sub_gt', seed=[691136], cats=True,
                   subcats=1, supercats=1)
g.set_working_graph('Algorithms_1_super_1_sub_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
#RTL(g).snapshots('snapshot_year', seed=22313095, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')

g.set_working_graph('main')
SubGraph(g).create(title='Data_Mining_2_sub_gt', seed=[5206601], cats=True,
                   subcats=2, supercats=0)
g.set_working_graph('Data_Mining_2_sub_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
#RTL(g).snapshots('snapshot_year', seed=22313095, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')

g.set_working_graph('main')
SubGraph(g).create(title='Data_Mining_2_super_gt', seed=[5206601], cats=True,
                   subcats=0, supercats=2)
g.set_working_graph('Data_Mining_2_super_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
#RTL(g).snapshots('snapshot_year', seed=22313095, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')

g.set_working_graph('main')
SubGraph(g).create(title='Data_Mining_1_sub_1_super_gt', seed=[5206601], cats=True,
                   subcats=1, supercats=1)
g.set_working_graph('Data_Mining_1_sub_1_super_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
#RTL(g).snapshots('snapshot_year', seed=22313095, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')


g.set_working_graph('main')
SubGraph(g).create(title='Machine_Learning_2_super_gt', seed=[706543], cats=True,
                   subcats=0, supercats=2)
g.set_working_graph('Machine_Learning_2_super_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
#RTL(g).snapshots('snapshot_year', seed=22313095, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')

g.set_working_graph('main')
SubGraph(g).create(title='Machine_Learning_2_sub_gt', seed=[706543], cats=True,
                   subcats=2, supercats=0)
g.set_working_graph('Machine_Learning_2_sub_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
#RTL(g).snapshots('snapshot_year', seed=22313095, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')

g.set_working_graph('main')
SubGraph(g).create(title='Machine_Learning_1_super_1_sub_gt', seed=[706543], cats=True,
                   subcats=1, supercats=1)
g.set_working_graph('Machine_Learning_1_super_1_sub_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
#RTL(g).snapshots('snapshot_year', seed=22313095, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
'''

'''
g.set_working_graph('main')
SubGraph(g).create(title='Digital_media_5sub_gt', seed=[1017992], cats=True,
                   subcats=5, supercats=0)
g.set_working_graph('Digital_media_5sub_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
RTL(g).snapshots('snapshot_year', seed=39563179, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')

'''

'''
g.set_working_graph('main')
SubGraph(g).create(title='Ethereum_article_1_super_gt', seed=[41754003], cats=True,
                   subcats=0, supercats=1)
g.set_working_graph('Ethereum_article_1_super_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
#RTL(g).snapshots('snapshot_year', seed=28249265, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')

g.set_working_graph('main')
SubGraph(g).create(title='Ethereum_article_2_super_gt', seed=[41754003], cats=True,
                   subcats=0, supercats=2)
g.set_working_graph('Ethereum_article_2_super_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
#RTL(g).snapshots('snapshot_year', seed=28249265, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')

g.set_working_graph('main')
SubGraph(g).create(title='Blockchain_article_1_super_gt', seed=[44065971], cats=True,
                   subcats=0, supercats=1)
g.set_working_graph('Blockchain_article_1_super_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
#RTL(g).snapshots('snapshot_year', seed=28249265, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')

g.set_working_graph('main')
SubGraph(g).create(title='Blockchain_article_2_super_gt', seed=[44065971], cats=True,
                   subcats=0, supercats=2)
g.set_working_graph('Blockchain_article_2_super_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
#RTL(g).snapshots('snapshot_year', seed=28249265, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
'''


g.set_working_graph('main')
SubGraph(g).create(title='Machine_Learning_1_sub_gt', seed=[706543], cats=True,
                   subcats=1, supercats=0)
g.set_working_graph('Machine_Learning_1_sub_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
#RTL(g).snapshots('snapshot_year', seed=22313095, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')

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


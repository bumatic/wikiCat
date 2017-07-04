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
SubGraph(g).create(title='Geosocial_networking_3_sub_1_super_gt', seed=[26593966], cats=True,
                   subcats=3, supercats=1)

g.set_working_graph('Geosocial_networking_3_sub_1_super_gt')
SubGraphProcessor(g).create_gt_subgraph()
Snapshots(g).create('snapshot_year')
SubGraphProcessor(g).internalize_snapshots('snapshot_year')
#RTL(g).snapshots('snapshot_year', seed=22313095, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')






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


from wikiCat.wikiproject import Project
import pprint
from wikiCat.processors.parsed_data_error_check import ParsedDataErrorCheck
from wikiCat.processors.graph_data_gererator import GraphDataGenerator
from wikiCat.processors.spark_processor_parsed import SparkProcessorParsed
from wikiCat.processors.controvercy_score import ControvercyScore
from wikiCat.processors.oldest_revision import OldestRevision
from wikiCat.processors.gt_graph_generator import GtGraphGenerator
from wikiCat.selectors.selector import Selector
from wikiCat.data.wikigraph import WikiGraph

pp = pprint.PrettyPrinter(indent=4)

mp = Project('project')
mp.load_project()
#mp.find_start_date()

#mp.set_dump_date('2016-11-01')

#mp.find_start_date()
#mp.set_dump_date('2016-11-01')
#pp.pprint(mp.pinfo)

#WikiGraph(mp).
#GtGraphGenerator(mp).register_gt_graph()

from wikiCat.selectors.selector import *

g = mp.gt_graph_objs['fixed_none__errors_removed']
g.set_working_graph()

Snapshots(g).create('snapshot_year_all')

#Selector(g).create_snapshot_views_spark()

#Selector(mp).temporal_views_spark()

#ParsedDataErrorCheck(mp, 'cat_data').missing_info_source_ids()
#ParsedDataErrorCheck(mp, 'cat_data').find_unresolvable_target_titles()

#mp.find_oldest_revision()



#GraphDataGenerator(mp, 'cat_data').generate_graph_data(override=True)

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


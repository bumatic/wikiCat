from wikiCat.wikiproject import Project
import pprint
from wikiCat.processors.parsed_data_error_check import ParsedDataErrorCheck
from wikiCat.processors.graph_data_gererator import GraphDataGenerator
from wikiCat.processors.spark_processor_parsed import SparkProcessorParsed
from wikiCat.processors.controvercy_score import ControvercyScore


pp = pprint.PrettyPrinter(indent=4)

mp = Project('project')
mp.load_project()

#pp.pprint(mp.pinfo)

#ParsedDataErrorCheck(mp, 'cat_data').missing_info_source_ids()
#ParsedDataErrorCheck(mp, 'cat_data').find_unresolvable_target_titles()

GraphDataGenerator(mp, 'cat_data').generate_graph_data(override=True)

#cat_data_fixed_none_errors_removed_1_edge

cs = ControvercyScore(mp)
cs.calculate_edge_score()
cs.caculate_node_score()


pp.pprint(mp.pinfo)
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


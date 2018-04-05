from wikiCat.wikiproject import Project

# To be included into readme

mp = Project(project_path='old_project')

# Creating a new project
'''
mp = Project(init=True)
mp.set_title = 'My Project'
mp.set_dump_date = ''
'''

# Adding parsed data to a project
'''
mp.add_parsed_data(page_info='orig_enwiki-20161101_page_info.csv', revision_info='orig_enwiki-20161101_revisions.csv', cat_data='orig_enwiki-20161101_cats.csv', link_data='')
'''

# Adding graph data to a project
'''
mp.add_graph_data('cat_data_fixed_none_errors_removed_1_nodes.csv', 'cat_data_fixed_none_errors_removed_1_edges.csv', 'cat_data_fixed_none_errors_removed_1_events.csv')
'''

# Adding main GT Graph to the project
'''
mp.add_gt_graph('gt_graph.gt', 'gt_nodes_graph.csv')
'''

# Generate graph data from parsed data
'''
mp.generate_graph_data(data_type='cats')
'''

# Find errors in parsed data
'''
mp.find_errors_in_parsed()
'''

# Find first revision in data and use as start date for the project
'''
mp.find_start_date()
'''

# Calculate Cscores
'''
mp.calculate_cscores()
'''

# Generate gt_graph from graph data
'''
mp.generate_gt_graph()
'''

# Accessing gt_graph
'''
mp.graph
'''

# Listing available gt_graphs:
'''
mp.graph.list_graphs()
'''

# Setting working graph
'''
mp.graph.set_working_graph('main')
'''

# Get current working graph
'''
mp.graph.get_working_graph()
'''

# Create Subgraph
'''
#mp.graph.set_working_graph('main')
mp.create_subgraph(title='Simulation_1_sub_1_super_gt', seed=[7744777], cats=True,
                   subcats=1, supercats=1)
'''

# Create Snapshots
'''
mp.create_snapshots(graph='Simulation_1_sub_1_super_gt', title='snapshots_year',
                    start_date='2003-01-10')
'''

# Remove Snapshots
'''
mp.remove_snapshots(subgraph='Simulation_1_sub_1_super_gt', title='Simulation_1_sub_1_super_gt')
'''

# Remove Subgraph
'''
mp.remove_subgraph(subgraph= ...) #untested
'''

mp.create_static_viz('Simulation_1_sub_1_super_gt', 'FR', snapshots='snapshots_year', drawing_props_file='static.json')

#mp.create_static_viz('Simulation_1_sub_1_super_gt', 'RTL', snapshots='snapshots_year',
#                     drawing_props_file='static.json', seed=7744777)
#RTL(g).snapshots('snapshot_year', seed=22313095, vsize='cscore', vlabel='title', esize='cscore')


# Visualize


#RTL(g).snapshots('snapshot_year', seed=22313095, vsize='cscore', vlabel='title', esize='cscore')
#SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
#ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')




'''
import pprint
import json
from wikiCat.processors.parsed_data_error_check import ParsedDataErrorCheck

pp = pprint.PrettyPrinter(indent=4)
#pp.pprint(myProject.pinfo)

#RTL(g).snapshots('snapshot_year', seed=22313095, vsize='cscore', vlabel='title', esize='cscore')
SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
'''


'''
mp = Project('project')
mp.load_project()
g = mp.gt_graph_objs['fixed_none__errors_removed']

g.set_working_graph('main')
#SubGraph(g).create(title='Simulation_1_sub_1_super_gt', seed=[7744777], cats=True,
#                   subcats=1, supercats=1)

g.set_working_graph('Simulation_1_sub_1_super_gt')
#SFDP(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')
#ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')

ARFVid(g).visualize(output_size=(1500,1500), vertex_size='cscore', vertex_min=10, vertex_max=30, vertex_text=True,
              vertex_font_size=None, vertex_color_by_type=True, edge_size='cscore', edge_min=1, edge_max=10)

#ARFVid(g).visualize(output_size=(500,500), vertex_size=10, vertex_min=None, vertex_max=None, vertex_text=True,
#              vertex_font_size=None, vertex_color_by_type=True, edge_size='cscore', edge_min=1, edge_max=10)




ARF(g).snapshots('snapshot_year', vsize='cscore', vlabel='title', esize='cscore')


def visualize(self, graph_view, outfile, outtype, vsize=None, vlabel=None, color_by_type=True, esize=None):





ReihenFolge

ARF(g).snapshots(self, stype, outtype='png', vsize=None, vlabel=None, color_by_type=True, esize=None):
       
    self.results_path = os.path.join(self.results_path, stype)
    
    snapshot_files = self.data[self.working_graph][stype]['files']
    snapshot_path = os.path.join(self.graph.data[self.working_graph]['location'], stype)
    
    #graph_view = self.create_snapshot_view(snapshot_path, file, stype)
    #self.visualize(graph_view, file[:-4], outtype, vsize=vsize, vlabel=vlabel, color_by_type=color_by_type, esize=esize)
    #

ARF.snapshots

arf.visualize(self, graph_view, outfile, outtype, vsize=None, vlabel=None, color_by_type=True, esize=None):






'''
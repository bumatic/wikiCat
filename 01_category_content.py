from wikiCat.wikiproject import Project


mp = Project()

# Contents 3sub
#mp.graph.set_working_graph(key='main')
#mp.create_subgraph(title='contents_3sub', seed=[14105005], cats=True,
#                   subcats=3, supercats=0)

mp.create_gt_subgraph(title='contents_3sub')
#mp.create_snapshots(graph='contents_3sub', title='snapshots_year')
#mp.create_static_viz('contents_3sub', 'SFDP', snapshots='snapshots_year', drawing_props_file='static.json')



# Contents 5sub
'''
mp.graph.set_working_graph(key='main')
mp.create_subgraph(title='contents_5sub', seed=[14105005], cats=True,
                   subcats=5, supercats=0)
mp.create_snapshots(graph='contents_5sub', title='snapshots_year')
mp.create_static_viz('contents_5sub', 'SFDP', snapshots='snapshots_year', drawing_props_file='static.json')
'''

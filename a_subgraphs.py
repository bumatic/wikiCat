from wikiCat.wikiproject import Project
mp = Project()



# https://en.wikipedia.org/wiki/Category:Brexit : 53543636


'''
mp.graph.set_working_graph(key='main')
mp.create_subgraph(title='dh_2super', seed=[3900832], cats=True,
                   subcats=0, supercats=2)
mp.create_snapshots(graph='dh_2super', title='snapshots_year')
mp.create_static_viz('dh_2super', 'SFDP', snapshots='snapshots_year', drawing_props_file='static.json')


mp.graph.set_working_graph(key='main')
mp.create_subgraph(title='dh_cat_1_sub_1super', seed=[33395167], cats=True,
                   subcats=1, supercats=1)
'''
#mp.create_snapshots(graph='dh_cat_1_sub_1super', title='snapshots_year')
mp.create_static_viz('dh_cat_1_sub_1super', 'SFDP', snapshots='snapshots_year', drawing_props_file='static.json')


'''
mp.graph.set_working_graph(key='main')
mp.create_subgraph(title='cat_brexit_1_sub_1super', seed=[53543636], cats=True,
                   subcats=1, supercats=1)
mp.create_snapshots(graph='cat_brexit_1_sub_1super', title='snapshots_year')
mp.create_static_viz('cat_brexit_1_sub_1super', 'SFDP', snapshots='snapshots_year', drawing_props_file='static.json')

mp.graph.set_working_graph(key='main')
mp.create_subgraph(title='cat_brexit_2_sub', seed=[53543636], cats=True,
                   subcats=2, supercats=0)
mp.create_snapshots(graph='cat_brexit_2_sub', title='snapshots_year')
mp.create_static_viz('cat_brexit_2_sub', 'SFDP', snapshots='snapshots_year', drawing_props_file='static.json')

mp.graph.set_working_graph(key='main')
mp.create_subgraph(title='cat_brexit_2_sub_1super', seed=[53543636], cats=True,
                   subcats=2, supercats=1)
mp.create_snapshots(graph='cat_brexit_2_sub_1super', title='snapshots_year')
mp.create_static_viz('cat_brexit_2_sub_1super', 'SFDP', snapshots='snapshots_year', drawing_props_file='static.json')
'''

'''
mp.graph.set_working_graph(key='main')
mp.create_subgraph(title='history_of_computing_1super', seed=[1506937], cats=True,
                   subcats=0, supercats=1)
mp.create_snapshots(graph='history_of_computing_1super', title='snapshots_year')
mp.create_static_viz('history_of_computing_1super', 'SFDP', snapshots='snapshots_year', drawing_props_file='static.json')

mp.graph.set_working_graph(key='main')
mp.create_subgraph(title='history_of_computing_2sub', seed=[1506937], cats=True,
                   subcats=2, supercats=0)
mp.create_snapshots(graph='history_of_computing_2sub', title='snapshots_year')
mp.create_static_viz('history_of_computing_2sub', 'SFDP', snapshots='snapshots_year', drawing_props_file='static.json')

mp.graph.set_working_graph(key='main')
mp.create_subgraph(title='algorithms_1sub_1super', seed=[691136], cats=True,
                   subcats=1, supercats=1)
mp.create_snapshots(graph='algorithms_1sub_1super', title='snapshots_year')
mp.create_static_viz('algorithms_1sub_1super', 'SFDP', snapshots='snapshots_year', drawing_props_file='static.json')

mp.graph.set_working_graph(key='main')
mp.create_subgraph(title='artificial_intelligence_1sub_1super', seed=[700355], cats=True,
                   subcats=1, supercats=1)

mp.create_snapshots(graph='artificial_intelligence_1sub_1super', title='snapshots_year')
mp.create_static_viz('artificial_intelligence_1sub_1super', 'SFDP', snapshots='snapshots_year', drawing_props_file='static.json')
'''
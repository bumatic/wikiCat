from wikiCat.wikiproject import Project
mp = Project()

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
'''
mp.create_snapshots(graph='artificial_intelligence_1sub_1super', title='snapshots_year')
mp.create_static_viz('artificial_intelligence_1sub_1super', 'SFDP', snapshots='snapshots_year', drawing_props_file='static.json')

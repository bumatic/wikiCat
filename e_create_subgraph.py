from wikiCat.wikiproject import Project

mp = Project()

# Contents 1sub 1super

mp.graph.set_working_graph(key='main')
mp.create_subgraph(title='contents_2sub', seed=[14105005], cats=True,
                   subcats=2, supercats=0)
mp.create_snapshots(graph='contents_2sub', title='snapshots_year')
#mp.create_static_viz('contents_2sub', 'SFDP', snapshots='snapshots_year', drawing_props_file='static.json')

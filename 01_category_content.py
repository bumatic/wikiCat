from wikiCat.wikiproject import Project


mp = Project()

# Find errors in parsed data
mp.find_errors_in_parsed()

# Contents 1sub 1super
mp.create_subgraph(title='contents_1sub_1super', seed=[14105005], cats=True,
                   subcats=1, supercats=1)
mp.create_snapshots(graph='contents_1sub_1super', title='snapshots_year')
mp.create_static_viz('contents_1sub_1super', 'SFDP', snapshots='snapshots_year', drawing_props_file='static.json')

# Contents 3sub
mp.create_subgraph(title='contents_3sub', seed=[14105005], cats=True,
                   subcats=3, supercats=0)
mp.create_snapshots(graph='contents_3sub', title='snapshots_year')
mp.create_static_viz('contents_3sub', 'SFDP', snapshots='snapshots_year', drawing_props_file='static.json')

# Contents 3sub
mp.create_subgraph(title='contents_5sub', seed=[14105005], cats=True,
                   subcats=5, supercats=0)
mp.create_snapshots(graph='contents_5sub', title='snapshots_year')
mp.create_static_viz('contents_5sub', 'SFDP', snapshots='snapshots_year', drawing_props_file='static.json')


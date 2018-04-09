from wikiCat.wikiproject import Project


mp = Project()
# All
mp.create_snapshots(title='first_year_snapshots_day', slice='day', start_date='2003-01-05', end_date='2004-01-05')
mp.create_static_viz('main', 'SFDP', snapshots='first_year_snapshots_day', drawing_props_file='static.json')
mp.create_snapshots(title='snapshots_year', slice='year')
#mp.create_static_viz('main', 'SFDP', snapshots='first_year_snapshots_day', drawing_props_file='static.json')



# Bitcoin
mp.create_subgraph(title='bitcoin_1sub_1super', seed=[39563179], cats=True,
                   subcats=1, supercats=1)
mp.create_snapshots(graph='bitcoin_1sub_1super', title='snapshots_year')
mp.create_static_viz('bitcoin_1sub_1super', 'SFDP', snapshots='snapshots_year', drawing_props_file='static.json')


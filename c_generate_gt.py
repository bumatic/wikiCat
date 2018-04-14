from wikiCat.wikiproject import Project

mp = Project()

mp.remove_subgraph('main')
mp.generate_gt_graph()

'''
mp.graph.set_working_graph(key='main')
mp.create_snapshots(title='snapshots_month', slice='month')
mp.graph.set_working_graph(key='main')
mp.create_snapshots(title='snapshots_year', slice='year')
mp.graph.set_working_graph(key='main')
mp.create_snapshots(title='first_year_snapshots_day', slice='day', start_date='2003-01-05', end_date='2004-01-05')
'''
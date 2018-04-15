from wikiCat.wikiproject import Project

mp = Project()
mp.generate_gt_graph()

mp.graph.set_working_graph(key='main')
mp.create_snapshots(title='snapshots_month', slice='month')
mp.graph.set_working_graph(key='main')
mp.create_snapshots(title='snapshots_year', slice='year')
mp.graph.set_working_graph(key='main')
mp.create_snapshots(title='snapshots_day_may_2004', slice='day', start_date='2004-05-01', end_date='2004-06-01')
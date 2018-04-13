from wikiCat.wikiproject import Project

mp = Project(project_path='test_project')

mp.generate_graph_data()
mp.find_start_date()
mp.set_dump_date('2018-03-01')
#mp.create_subgraph(title='test', seed=[12], subcats=5)
#mp.create_snapshots(graph='test', start_date='2004-06-02')

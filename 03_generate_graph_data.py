from wikiCat.wikiproject import Project

#mp = Project(init=True)
#mp.set_title = 'WikiCat'
#mp.set_dump_date = '2018-03-01'

mp = Project()
mp.generate_graph_data(data_type='cats')

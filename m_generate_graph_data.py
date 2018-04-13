from wikiCat.wikiproject import Project

mp = Project(project_path='test_project')
#mp.add_parsed_data(page_info='pinfo.csv', revision_info='revs.csv', cat_data='cats.csv')

mp.generate_graph_data()
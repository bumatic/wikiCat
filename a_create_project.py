from wikiCat.wikiproject import Project
mp = Project(init=True)

mp.add_parsed_data(page_info='page_info.csv',
                   revision_info='revisions.csv',
                   author_info='author_info.csv',
                   cat_data='cats.csv',
                   link_data='',
                   description='Some description about the data.')

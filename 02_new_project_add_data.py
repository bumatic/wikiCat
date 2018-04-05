from wikiCat.wikiproject import Project

#mp = Project(init=True)
#mp.set_title = 'WikiCat'
#mp.set_dump_date = '2018-03-01'

mp = Project()
mp.add_parsed_data(page_info='enwiki-2018-03-01-page-info.csv',
                   revision_info='enwiki-2018-03-01-revisions.csv',
                   cat_data='enwiki-2018-03-01-cats.csv')


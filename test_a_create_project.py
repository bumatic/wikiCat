from wikiCat.wikiproject import Project
mp = Project(init=True)
mp = Project()
mp.add_parsed_data(page_info='page_info.csv',
                   revision_info='revisions.csv',
                   author_info='author_info.csv',
                   cat_data='cats.csv',
                   link_data=["enwiki-20180701-pages-meta-history19.xml-p17455823p17621007_links.csv.7z",
                              "enwiki-20180701-pages-meta-history9.xml-p1851539p1881365_links.csv.7z"],
                   description='enwiki dump July 2018')

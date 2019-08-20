from wikiCat.wikiproject import Project

mp = Project()

mp.create_separate_subgraph(title='ict', seed=['50886144'],
                            cats=True, subcats=10, supercats=1,
                            links=True, inlinks=1, outlinks=1)


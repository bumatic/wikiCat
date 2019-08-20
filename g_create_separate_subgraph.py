from wikiCat.wikiproject import Project

mp = Project()

mp.create_separate_subgraph(title='test', seed=['31778757', '1085860'],
                            cats=True, subcats=3, supercats=1,
                            links=True, inlinks=1, outlinks=1)


from wikiCat.wikiproject import Project

mp = Project()


mp.create_separate_subgraph(title='test', seed=['31778757', '1085860'],
                            cats=True, subcats=2, supercats=1,
                            links=True, inlinks=1, outlinks=1)


'''
mp.create_separate_subgraph(title='ict',
                            seed=['700813', '700911', '1257870', '2150811', '2959298', '5953713', '14787852', '21491261'],
                            cats=True, subcats=4, supercats=1,
                            links=True, inlinks=1, outlinks=1)
'''
from wikiCat.wikiproject import Project

mp = Project()
mp.create_subgraph(title='bitcoin_1sub_1super', seed=[39563179], cats=True,
                   subcats=1, supercats=1)
mp.create_snapshots(graph='bitcoin_1sub_1super', title='snapshots_year')

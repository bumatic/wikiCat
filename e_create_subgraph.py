from wikiCat.wikiproject import Project

mp = Project()
mp.create_snapshots(graph='main', title='snapshots_month_2004_2005', slice='month', end_date='2005-12-31')


mp.graph.set_working_graph(key='main')
mp.create_subgraph(title='blockchains_cat_1sub_1super', seed=[48210587], cats=True,
                   subcats=1, supercats=1)
mp.create_snapshots(graph='blockchains_cat_1sub_1super', title='snapshots_year')

mp.graph.set_working_graph(key='main')
mp.create_subgraph(title='blockchain_2super', seed=[44065971], cats=True,
                   subcats=0, supercats=2)
mp.create_snapshots(graph='blockchain_2super', title='snapshots_year')

mp.graph.set_working_graph(key='main')
mp.create_subgraph(title='bitcoin_blockchain_ethereum_2super', seed=[28249265, 44065971, 41754003], cats=True,
                   subcats=0, supercats=2)
mp.create_snapshots(graph='bitcoin_blockchain_ethereum_2super', title='snapshots_year')

mp.graph.set_working_graph(key='main')
mp.create_subgraph(title='bitcoin_2super', seed=[2824926], cats=True,
                   subcats=0, supercats=2)
mp.create_snapshots(graph='bitcoin_2super', title='snapshots_year')

mp.graph.set_working_graph(key='main')
mp.create_subgraph(title='brexit_2super', seed=[41688778], cats=True,
                   subcats=0, supercats=2)
mp.create_snapshots(graph='brexit_2super', title='snapshots_year')


#Contents 1_sub
mp.graph.set_working_graph(key='main')
mp.create_subgraph(title='contents_1sub', seed=[14105005], cats=True,
                   subcats=1, supercats=0)
mp.create_snapshots(graph='contents_1sub', title='snapshots_year')


mp.graph.set_working_graph(key='main')
mp.create_subgraph(title='contents_1super', seed=[14105005], cats=True,
                   subcats=0, supercats=1)
mp.create_snapshots(graph='contents_1super', title='snapshots_year')


'''
#Interesting Pages/ Categories

Category:Blockchains	48210587
Category:Big Data	40423498
Category:Machine Learning	706543
Category:Data Mining	5206601
Category:Artificial Intelligence	700355
Category:Algorithms		691136


##Articles
Cryptocurrency		41684201
Bitcoin			28249265
Blockchain		44065971
Ethereum			41754003

Brexit			41688778

Prediction			1436435
Omics		 	13693058
'''

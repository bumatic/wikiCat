from datetime import datetime
from wikiCat.wikiproject import Project

#mp = Project(init=True)
#mp.set_title = 'WikiCat'
#mp.set_dump_date = '2018-03-01'


mp = Project()

#mp.add_graph_data('test_nodes.csv', 'test_edges.csv', 'test_events.csv')
#mp.find_start_date()
#mp.calculate_cscores()
mp.generate_gt_graph()

#mp.create_subgraph('testtest', [10010944])

#end = datetime.now()
#duration = end-start
#print('Duration:')
#print(str(duration.min) +':'+str(duration.seconds))

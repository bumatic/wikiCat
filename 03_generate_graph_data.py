from datetime import datetime
from wikiCat.wikiproject import Project

#mp = Project(init=True)
#mp.set_title = 'WikiCat'
#mp.set_dump_date = '2018-03-01'

start = datetime.now()
mp = Project()
#mp.generate_graph_data(data_type='cats')
#mp.add_graph_data('cats_1_nodes.csv', 'cats_1_edges.csv', 'cats_1_events.csv')
mp.find_start_date()
mp.calculate_cscores()
mp.generate_gt_graph()
end = datetime.now()
duration = end-start
print('Duration:')
print(str(duration.min) +':'+str(duration.seconds))

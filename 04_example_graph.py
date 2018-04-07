from datetime import datetime
from wikiCat.wikiproject import Project

#mp = Project(init=True)
#mp.set_title = 'WikiCat'
#mp.set_dump_date = '2018-03-01'


mp = Project()
mp.create_snapshots(title='first_year_snapshots_day', slice='day', start_date='2003-01-05', end_date='2004-01-05')
mp.create_static_viz('main', 'ARF', snapshots='first_year_snapshots_day', drawing_props_file='static.json')


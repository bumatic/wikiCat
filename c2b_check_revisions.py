import os
import pandas as pd

event_data_file = os.path.join('manual_graph_data_generation', 'enwiki-20180701-pages-meta-history10.xml-p2505803p2535938_links_events.csv')
event_data = pd.read_csv(event_data_file, delimiter='\t', names=['source', 'target', 'rev_id', 'event'])


chunksize = 1000000
rev_data_file = os.path.join('project', '01_data', '01_parsed', 'revisions.csv')
rev_data = pd.read_csv(rev_data_file, delimiter='\t', names=['page_id', 'rev_id', 'ts', 'author_id'], chunksize=chunksize)

for chunk in rev_data:
    print(event_data.merge(chunk, left_on='rev_id', right_on='rev_id'))

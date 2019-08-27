import os
import pandas as pd





rev_data_file = os.path.join('manual_graph_data_generation', 'revisions_parsed_new.csv')
rev_data = pd.read_csv(rev_data_file, delimiter='\t', names=['page_id', 'rev_id', 'ts', 'author_id'])

print(len(rev_data))

rel_rev_file = os.path.join('manual_graph_data_generation', 'relevant_revisions_parsed_new.csv')
rel_rev = pd.read_csv(rel_rev_file, delimiter='\t', names=['rev_id'])
print(len(rel_rev))

rev_new = rev_data.merge(rel_rev, on='rev_id')
print(len(rev_new))

rev_new.to_csv('revisions_reduced_new.csv', index=None, sep='\t', header=None)




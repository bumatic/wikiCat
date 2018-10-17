import pandas as pd
import os
from tqdm import tqdm

directory = os.path.join('project', '01_data', '02_data_graph')
file = 'events.csv'
source = os.path.join(directory, file)
column_names = ['ts', 'source', 'target', 'event', 'author']

chunksize = 1000000
results = pd.DataFrame()
for chunk in pd.read_csv(source, file, header=None, delimiter='\t', names=column_names,
                         na_filter=False, chunksize=chunksize):
    results = results.append(chunk.drop(columns=['ts', 'event', 'author']).drop_duplicates())

print('Number of unique edges in ' + file + ': ' + str(results.drop_duplicates().shape[0]))



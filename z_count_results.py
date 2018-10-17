import pandas as pd
import os
from tqdm import tqdm

directory = os.path.join('project', '01_data', '02_data_graph')
files = ['events.csv', 'edges.csv', 'nodes.csv']


for file in tqdm(files):
    source = os.path.join(directory, file)
    chunksize = 1000000
    counter = 0
    for chunk in tqdm(pd.read_csv(source, file, header=None, delimiter='\t', na_filter=False, chunksize=chunksize)):
        counter = counter + chunk.shape[0]
    print('Number of rows in ' + file + ': ' + str(counter))

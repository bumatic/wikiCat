import pandas as pd
import os

directory = os.path.join('project', '01_data', '02_data_graph')
files = ['events.csv', 'edges.csv', 'nodes.csv']


for file in files:
    source = os.path.join(directory, file)
    chunksize = 1000000
    counter = 0
    for chunk in pd.read_csv(source, file, header=None, delimiter='\t', na_filter=False, chunksize=chunksize):
        counter = counter + chunk.shape[0]
    print('Number of rows in ' + file + ': ' + str(counter))

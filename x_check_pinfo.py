from wikiCat.wikiproject import Project
import pandas as pd
import os

mp = Project()
print(mp.pinfo)

nodes = pd.read_csv(os.path.join('project', '01_data', '02_data_graph', 'nodes.csv'), delimiter='\t', header=None,
                    names=['id', 'title', 'ns', 'cscore'], na_filter=False)

x = nodes[nodes['cscore'].isin([''])]
print(x)

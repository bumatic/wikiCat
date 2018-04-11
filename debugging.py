#from wikiCat.wikiproject import Project
import pandas as pd

path = 'project/02_gt_graphs/contents_3sub/contents_3sub_cats_1_edges.csv'

dtype = {
    'source': int,
    'target': int,
    'type': str,
    'cscore': float
        }

df =pd.read_csv(path, header=None, delimiter='\t', names=['source', 'target', 'type', 'cscore'],
                dtype=dtype, na_filter=False)
df = df[['source', 'target']]
print(df)

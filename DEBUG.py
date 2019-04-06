import pandas as pd
import os
from dateutil import parser


date = 1016010777.0

# For Testdata
#file = os.path.join('project', '01_data', '01_parsed', 'r.csv')
#revs = pd.read_csv(file, header=None, sep='\t', names=['err', 'source', 'rev', 'date', 'author'])
#revs = revs[['source', 'rev', 'date', 'author']]
#date = parser.parse('2004-03-18T02:30:47Z').timestamp()

# For regular data
file = os.path.join('project', '01_data', '01_parsed', 'revisions.csv')
revs = pd.read_csv(file, header=None, sep='\t', names=['source', 'rev', 'date', 'author'])

chunksize = 1000000
revisions = pd.DataFrame()
for revs in pd.read_csv(file, header=None, sep='\t', names=['err', 'source', 'rev', 'date', 'author'], chunksize=chunksize):
    revs['datetime'] = revs['date'].apply(lambda x: parser.parse(x).timestamp())
    if len(revs[revs['datetime'] == date]) > 0:
        print(revs[revs['datetime'] == date])
        revisions = revisions.append(revs[revs['datetime'] == date])
revisions.to_csv('DEBUG_revisions.csv', sep='\t', header=False, index=False)
print(revisions)

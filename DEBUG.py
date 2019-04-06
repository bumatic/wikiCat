import pandas as pd
import os
from dateutil import parser


date = 1016010777.0

# For Testdata
#file = os.path.join('project', '01_data', '01_parsed', 'r.csv')
#revs = pd.read_csv(file, header=None, sep='\t', names=['err', 'source', 'rev', 'date', 'author'])
#revs = revs[['source', 'rev', 'date', 'author']]

# For regular data
file = os.path.join('project', '01_data', '01_parsed', 'revisions.csv')
revs = pd.read_csv(file, header=None, sep='\t', names=['source', 'rev', 'date', 'author'])


revs['datetime'] = revs['date'].apply(lambda x: parser.parse(x).timestamp())
print(revs[revs['datetime'] == date])

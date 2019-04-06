import pandas as pd
import os
from dateutil import parser

#  Debugging
file = os.path.join('project', '01_data', '01_parsed', 'revisions.csv')
date = 1016010777.0
# Wednesday, 13. March 2002 09:12:57
date = '2002-03-13T09:12:57Z'

names = ['source', 'rev', 'date', 'author']
chunksize = 10000000

test = False

# Debugging Test:
if test:
    file = os.path.join('project', '01_data', '01_parsed', 'r.csv')
    date = parser.parse('2004-03-18T02:30:47Z').timestamp()
    names = ['err', 'source', 'rev', 'date', 'author']
    chunksize = 10


revisions = pd.DataFrame()
for revs in pd.read_csv(file, header=None, sep='\t', names=names, chunksize=chunksize):
    #revs['datetime'] = revs['date'].apply(lambda x: parser.parse(x).timestamp())
    #if len(revs[revs['datetime'] == date]) > 0:
    if len(revs[revs['date'] == date]) > 0:
        print(revs[revs['date'] == date])
        revisions = revisions.append(revs[revs['date'] == date])
    else:
        print(False)
revisions.to_csv('DEBUG_revisions.csv', sep='\t', header=False, index=False)
print(revisions)

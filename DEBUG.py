import pandas as pd
import os
from dateutil import parser


#  Debugging
rev_file = os.path.join('project', '01_data', '01_parsed', 'revisions.csv')
#date = 1016010777.0 # Wednesday, 13. March 2002 09:12:57
date = '2002-03-13T09:12:57Z' # Wednesday, 13. March 2002 09:12:57

names = ['source', 'rev', 'date', 'author']
chunksize = 10000000
test = False

'''
# Debugging Test:
if test:
    rev_file = os.path.join('project', '01_data', '01_parsed', 'r.csv')
    date = parser.parse('2004-03-18T02:30:47Z').timestamp()
    names = ['err', 'source', 'rev', 'date', 'author']
    chunksize = 10


revisions = pd.DataFrame()
for revs in pd.read_csv(rev_file, header=None, sep='\t', names=names, chunksize=chunksize):
    #revs['datetime'] = revs['date'].apply(lambda x: parser.parse(x).timestamp())
    #if len(revs[revs['datetime'] == date]) > 0:
    if len(revs[revs['date'] == date]) > 0:
        print(revs[revs['date'] == date])
        revisions = revisions.append(revs[revs['date'] == date])
    else:
        print(False)
revisions.to_csv('DEBUG_revisions.csv', sep='\t', header=False, index=False)
print(revisions)
'''

#Results
#24032   304105  2002-03-13T09:12:57Z    76.0

import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql import Row
from dateutil import parser
#import shutil


def mapper_revisions(line):
    fields = line.split('\t')
    rev_id = int(fields[1])
    try:
        rev_date = parser.parse(fields[2])
        rev_date = rev_date.timestamp()
    except ValueError as e:
        print("Value Error at TS:", e, " in row", fields)
        try:
            rev_date = parser.parse(fields[2][:-1])
            rev_date = rev_date.timestamp()
        except ValueError as e:
            return
    try:
        rev_author = int(float(str(fields[3])))
    except ValueError as e:
        rev_author = -1
        # print("error", e, "for data ", fields)
    except IndexError as e:
        rev_author = -1
    return Row(rev_id=rev_id, rev_date=rev_date, rev_author=rev_author)

def mapper_author_info(line):
    fields = line.split('\t')
    try:
        author_id = int(float(str(fields[0])))
    except ValueError as e:
        print("error", e, "for data ", fields)
        author_id = -2
    author_name = str(fields[1])
    return Row(author_id=author_id, author_name=author_name)


spark = SparkSession\
    .builder \
    .appName("Generate_Graph_Data") \
    .config("spark.driver.memory", "60g") \
    .config("spark.driver.maxResultSize", "60g") \
    .getOrCreate()

author_file = os.path.join('project', '01_data', '01_parsed', 'author_info.csv')

author_info_source = spark.sparkContext.textFile(os.path.join(author_file))
author_info = author_info_source.map(mapper_author_info)
author_info_df = spark.createDataFrame(author_info).cache()
missing_author_row = spark.createDataFrame([[-1, "NO_AUTHOR_DATA"]])
author_info_df = author_info_df.union(missing_author_row)
author_info_df.createOrReplaceTempView("author")

revision_info_source = spark.sparkContext.textFile(rev_file)
revision_info = revision_info_source.map(mapper_revisions)
revision_info_df = spark.createDataFrame(revision_info).cache()
revision_info_df.createOrReplaceTempView("revision")

revision_info_df = spark.sql("SELECT * FROM revision WHERE rev_author = 76.0")
revision_info_df.show()
revision_info_df.createOrReplaceTempView("revision")

resolved_authors_df = spark.sql('SELECT r.rev_id, r.rev_date, rev_author, a.author_name as rev_author_name '
                                'FROM revision r LEFT OUTER JOIN author a ON r.rev_author = a.author_id')
resolved_authors_df.show()

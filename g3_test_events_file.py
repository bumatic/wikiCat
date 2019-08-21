import os

from dateutil import parser
from datetime import datetime
from wikiCat.wikiproject import Project
from wikiCat.selector.selector import Selector, SparkProcessorGraph

import findspark
findspark.init()
from pyspark.sql import SparkSession, Row
from pyspark import SparkConf, SparkContext

from pyspark.sql.functions import col


mp = Project()
f = 'project/01_data/02_data_graph/enwiki-20180701-pages-meta-history10.xml-p2505803p2535938_links_events.csv'
if os.path.isfile(f):
    print('file exists')


def mapper_events(line):
    fields = line.split('\t')
    if len(fields) == 5:
        revision = float(fields[0])
        source = fields[1]
        target = fields[2]
        event = fields[3]
        author = str(fields[4])
        return Row(revision=revision, source=source, target=target, event=event, author=author)
    elif len(fields) == 6:
        revision = float(fields[0])
        source = fields[1]
        target = fields[2]
        event = fields[3]
        author = fields[4]
        cscore = fields[5]
        return Row(revision=revision, source=source, target=target, event=event, author=author, cscore=cscore)
    else:
        print('Error while mapping events')
        return


spark = SparkSession \
            .builder \
            .appName("Generate_Separate_Subgraph_Data") \
            .config("spark.driver.memory", "60g") \
            .config("spark.driver.maxResultSize", "60g") \
            .getOrCreate()

print(SparkConf().getAll())


data = mp.pinfo['data']['graph']
graph_path = mp.pinfo['path']['graph']


events_source = spark.sparkContext.textFile(f)

print('begin')
print(events_source)
print('============================================================')

events = events_source.map(mapper_events)

events_df = spark.createDataFrame(events).cache()

events_df.show()

print('============================================================')
print(events_source)
print('end')
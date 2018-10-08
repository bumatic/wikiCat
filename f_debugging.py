import os
import findspark
findspark.init()
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import collect_list, avg, max
from wikiCat.processor.pandas_processor_graph import PandasProcessorGraph
from wikiCat.processor.spark_processor_graph import SparkProcessorGraph
from dateutil import parser



def mapper_revisions(line):
    fields = line.split('\t')
    rev_id = int(fields[1])
    rev_date = parser.parse(fields[2])
    rev_date = rev_date.timestamp()
    try:
        rev_author = int(float(fields[3]))
    except:
        rev_author = 0
        print(fields[3])
    return Row(rev_id=rev_id, rev_date=rev_date, rev_author=rev_author)

spark = SparkSession\
    .builder\
    .appName("Calculate_Controvercy_Score_Edges")\
    .config("spark.driver.memory", "70g")\
    .config("spark.driver.maxResultSize", "40g")\
    .getOrCreate()


print(SparkConf().getAll())

source = spark.sparkContext.textFile(os.path.join('project', '01_data', '01_parsed', 'revisions.csv'))
events = source.map(mapper_revisions)
events_df = spark.createDataFrame(events).cache()
#events_df.createOrReplaceTempView("events")
events_df = events_df.filter(events_df.rev_date == 1.390047722E9)
events_df.show()


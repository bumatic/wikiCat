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
    counter = 0
    try:
        rev_author = int(float(fields[3]))
    except:
        rev_author = 0
        counter+=1
        #print(fields)
    return Row(rev_id=rev_id, rev_date=rev_date, rev_author=rev_author)

spark = SparkSession\
    .builder\
    .appName("Calculate_Controvercy_Score_Edges")\
    .getOrCreate()

    # .config("spark.driver.maxResultSize", "40g")\
    # .config("spark.driver.memory", "70g")\

print(SparkConf().getAll())

source = spark.sparkContext.textFile(os.path.join('project', '01_data', '01_parsed', 'revisions.csv'))
print(source)

#tmp_results_file = os.path.join('project', '01_data', '01_parsed', 'tmp_results.csv')

events = source.map(mapper_revisions)
events_df = spark.createDataFrame(events).cache()
events_df.createOrReplaceTempView("events")
events_df.show()

# print(events_df.count())
# Result: 213087340

events_filtered_df = events_df.filter(events_df.rev_date == 1.390047722E9) #1.030213496E9
events_filtered_df.show()


'''
events_grouped_df = events_df.groupBy('source', 'target').agg(collect_list('revision').alias('revision'))
cscore_events = events_grouped_df.rdd.map(self.process_spark_list).collect()

# Processing list of cscore events and writing them to tmp file
cscore_events = [item for sublist in cscore_events for item in sublist]
print(cscore_events[-5:])

self.write_list(tmp_results_file, cscore_events)

cscore_events_source = spark.sparkContext.textFile(tmp_results_file)
cscore_events = cscore_events_source.map(self.mapper_tmp_cscore_events)
cscore_events_df = spark.createDataFrame(cscore_events).cache()
cscore_events_df.createOrReplaceTempView("cscore_events")

# ggf. nochmal mit Select DISTINCT probieren
resolved_event_type_df = spark.sql('SELECT e.revision, e.source, e.target, e.event, e.author, c.cscore '
                                   'FROM events e INNER JOIN cscore_events c '
                                   'ON (e.revision = c.revision AND e.source = c.source '
                                   'AND e.target = c.target)')

resolved_event_type_df = resolved_event_type_df.groupBy('revision', 'source', 'target', 'event', 'author')\
                .agg(max('cscore')).orderBy('revision', ascending=True)

resolved_event_type_df.show()
resolved_event_type_df.write.format('com.databricks.spark.csv').option('header', 'false').option('delimiter', '\t').save(spark_results_path)
os.remove(tmp_results_file)

#self.assemble_spark_results(spark_results_path, tmp_results_file)
#os.remove(os.path.join(self.data_path, file))
#os.rename(tmp_results_file, results_file)
'''

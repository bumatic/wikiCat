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

'''
# check which file is read in for processing

print(source)

# Result: project/01_data/01_parsed/revisions.csv MapPartitionsRDD[1] at textFile at NativeMethodAccessorImpl.java:0
'''

# tmp_results_file = os.path.join('project', '01_data', '01_parsed', 'tmp_results.csv')

events = source.map(mapper_revisions)
events_df = spark.createDataFrame(events).cache()
events_df.createOrReplaceTempView("events")

'''
# check structure in revisions.csv

events_df.show()

# Results:
# +----------+-------------+-------+
# |rev_author|     rev_date| rev_id|
# +----------+-------------+-------+
# |         0|1.006436661E9| 275670|
# |   8083618| 1.00651066E9| 275671|
# |         0|1.014651791E9| 172562|
# |         0|1.030213496E9| 172568|
# |         0|1.030213737E9| 423064|
# |       198|1.044798028E9| 712878|
# |      4444|1.052601796E9| 992695|
# |     10121|1.059464905E9|1231098|
# |     21737|1.063855724E9|1456275|
# |     13833|1.064047087E9|1463368|
# |      1926|1.064170102E9|1565240|
# |     10059|1.067589017E9|1781865|
# |     10059|1.069388259E9|1972324|
# |     21737|1.071723655E9|2124588|
# |      9475|1.073739349E9|2635411|
# |     21737|1.079577047E9|2812990|
# |     21737|1.079577063E9|2813083|
# |     21737|1.079577594E9|2989587|
# |     24180|1.080703494E9|3106710|
# |     13833|1.081401434E9|3525021|
# +----------+-------------+-------+
# only showing top 20 rows
'''

'''
# Check number of rows in revisions.csv

print(events_df.count())

# Result: 213087340
'''

'''
# Filter revisions for specific revision date in order to check for inconsistencies

events_filtered_df = events_df.filter(events_df.rev_date == 1.390047722E9) #1.030213496E9
events_filtered_df.show()

# Results:
# +----------+-------------+---------+
# |rev_author|     rev_date|   rev_id|
# +----------+-------------+---------+
# |    203786|1.390047722E9|591261883|
# +----------+-------------+---------+
'''


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

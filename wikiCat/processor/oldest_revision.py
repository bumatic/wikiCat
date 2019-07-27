from wikiCat.processor.spark_processor_graph import SparkProcessorGraph
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from datetime import datetime
import os


class OldestRevision(SparkProcessorGraph):
    def __init__(self, project): #, fixed='fixed_none', errors='errors_removed'
        SparkProcessorGraph.__init__(self, project) # , fixed=fixed, errors=errors
        if 'events' in self.project.pinfo['data']['graph'].keys():  # data_obj.data[self.data_status]:
            self.events_files = self.project.pinfo['data']['graph']['events']
        else:
            print('No csv with events available')
        if 'nodes' in self.project.pinfo['data']['graph'].keys():
            self.nodes_files = self.project.pinfo['data']['graph']['nodes']
        else:
            print('No csv with nodes available')
        if 'edges' in self.project.pinfo['data']['graph'].keys():
            self.edges_files = self.project.pinfo['data']['graph']['edges']
        else:
            print('No csv with edges available')

        #if 'gt' in self.data_obj.data[self.data_status]:
        #    self.gt_file = self.data_obj.data[self.data_status]['gt']
        #else:
        #    print('No graph_tool gt file available')

    def get(self, seed=None):
        # Create a SparkSession
        # Note: In case its run on Windows and generates errors use (tmp Folder mus exist):
        # spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("Postprocessing").getOrCreate()
        #spark = SparkSession.builder.appName("Oldest Revision").getOrCreate()

        spark = SparkSession \
            .builder \
            .appName("Calculate_Controvercy_Score_Edges") \
            .config("spark.driver.memory", "80g") \
            .config("spark.driver.maxResultSize", "80g") \
            .getOrCreate()
        print(SparkConf().getAll())

        # Infer the schema, and register the DataFrames as tables.
        revision_info_source = spark.sparkContext.textFile(os.path.join(self.data_path, self.events_files[0]))
        revision_info = revision_info_source.map(self.mapper_events)
        revision_info_df = spark.createDataFrame(revision_info).cache()

        if seed is not None:
            revision_info_df.createOrReplaceTempView("revisions")
            revision_info_df = spark.sql('SELECT * FROM revisions '
                                         'WHERE source = ' + str(seed) + ' OR target = ' + str(seed))

        # Get mininum revision
        minimum = revision_info_df.select('revision').rdd.min()[0]
        minimum = str(datetime.fromtimestamp(minimum))
        del spark
        return minimum

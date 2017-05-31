from wikiCat.processors.spark_processor import SparkProcessorGraph
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import min
from datetime import datetime
# from pyspark.sql import Row
# from dateutil import parser
# import collections
import os


class OldestRevision(SparkProcessorGraph):
    def __init__(self, project, fixed='fixed_none', errors='errors_removed'):
        SparkProcessorGraph.__init__(self, project)
        self.project = project
        self.path = self.project.graph_data_path
        self.fixed = fixed
        self.errors = errors
        self.data_status = 'graph__' + self.fixed + '__' + self.errors
        # self.data_obj[self.data_status]
        if 'events' in self.data_obj.data[self.data_status]:
            self.events_files = self.data_obj.data[self.data_status]['events']
        else:
            print('No csv with events available')
        if 'nodes' in self.data_obj.data[self.data_status]:
            self.nodes_files = self.data_obj.data[self.data_status]['nodes']
        else:
            print('No csv with nodes available')
        if 'edges' in self.data_obj.data[self.data_status]:
            self.edges_files = self.data_obj.data[self.data_status]['edges']
        else:
            print('No csv with edges available')
        if 'gt' in self.data_obj.data[self.data_status]:
            self.gt_file = self.data_obj.data[self.data_status]['gt']
        else:
            print('No graph_tool gt file available')


    def get(self):
        print(self.events_files)
        # Create a SparkSession
        # Note: In case its run on Windows and generates errors use (tmp Folder mus exist):
        # spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("Postprocessing").getOrCreate()
        spark = SparkSession.builder.appName("Oldest Revision").getOrCreate()

        # Infer the schema, and register the DataFrames as tables.
        revision_info_source = spark.sparkContext.textFile(os.path.join(self.data_path, self.events_files[0]))
        revision_info = revision_info_source.map(self.mapper_events)
        revision_info_df = spark.createDataFrame(revision_info).cache()
        minimum = revision_info_df.select('revision').rdd.min()[0]
        minimum = str(datetime.fromtimestamp(minimum))

        return minimum

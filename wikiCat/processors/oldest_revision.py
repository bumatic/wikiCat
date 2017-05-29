from wikiCat.processors.spark_processor_parsed import SparkProcessorParsed
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# from pyspark.sql import Row
# from dateutil import parser
# import collections
import os


class OldestRevision(SparkProcessorParsed):
    def __init__(self, project):
        SparkProcessorParsed.__init__(self, project)

    def get(self):
        # Create a SparkSession
        # Note: In case its run on Windows and generates errors use (tmp Folder mus exist):
        # spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("Postprocessing").getOrCreate()
        spark = SparkSession.builder.appName("Generate_Graph_Data").getOrCreate()

        # Infer the schema, and register the DataFrames as tables.
        revision_info_source = spark.sparkContext.textFile(os.path.join(self.data_path, self.revision_info))
        revision_info = revision_info_source.map(self.mapper_revisions)
        revision_info_df = spark.createDataFrame(revision_info).cache()
        revision_info_df.createOrReplaceTempView("revision")

        # Select oldest revision
        revision_info_df = spark.sql("SELECT min(rev_date) as revision from revision")
        results = revision_info_df.collect()
        return results

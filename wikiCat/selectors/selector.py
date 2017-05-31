import findspark
findspark.init()
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import collect_list, avg, col
from wikiCat.processors.pandas_processor_graph import PandasProcessorGraph
from wikiCat.processors.spark_processor import SparkProcessorGraph
from dateutil import parser
#import math
from datetime import datetime
#import pandas as pd
import os


class Selector(PandasProcessorGraph, SparkProcessorGraph):
    def __init__(self, project, fixed='fixed_none', errors='errors_removed'):
        PandasProcessorGraph.__init__(self, project, fixed=fixed, errors=errors)
        SparkProcessorGraph.__init__(self, project)
        self.start_date = self.project.start_date.timestamp()
        self.end_date = self.project.dump_date.timestamp()

    def temporal_views(self, slice='year', cscore=True, start_date=None):
        assert slice is 'year' or 'month' or 'day', 'Error. Pass a valid value for slice: year, month, day.'
        assert type(cscore) is bool, 'Error. A bool value is expected for cscore signalling, if data file contains ' \
                                     'cscore.'
        if start_date is not None:
            self.start_date = parser.parse(start_date).timestamp()
        if slice == 'day':
            delta = 86400
        elif slice == 'month':
            delta = 2592000
        elif slice == 'year':
            delta = 31536000
        results = {}
        tmp_results = {}
        last_slice = self.start_date.timestamp()
        for file in self.events_files:
            if cscore:
                self.load_events(file, columns=['revision', 'source', 'target', 'event', 'cscore'])
            else:
                self.load_events(file, columns=['revision', 'source', 'target', 'event'])
            for revision, events in self.events.groupby('revision'):
                if (revision - last_slice) > delta and revision >= self.start_date:
                    results[last_slice] = tmp_results
                    last_slice = revision
                for event in events.iterrows():
                    if event[1]['event'] == 'start':
                        tmp_results[str(event[1]['source'])+'|'+str(event[1]['target'])] = True
                    elif event[1]['event'] == 'end':
                        tmp_results[str(event[1]['source']) + '|' + str(event[1]['target'])] = False

        results[self.end_date] = tmp_results
        # TODO: Implement handling of results

    def generate_slice_list(self, slice):
        if slice == 'day':
            delta = 86400
        elif slice == 'month':
            delta = 2592000
        elif slice == 'year':
            delta = 31536000
        results = []
        curr_date = self.start_date
        while curr_date < self.end_date:
            results.append(curr_date)
            curr_date = curr_date + delta
        results.append(self.end_date)
        print(results)
        return results

    #TODO CHECK IF THIS WORKS!
    def temporal_views_spark(self, slice='year', cscore=True, start_date=None, end_date=None):
        assert slice is 'year' or 'month' or 'day', 'Error. Pass a valid value for slice: year, month, day.'
        assert type(cscore) is bool, 'Error. A bool value is expected for cscore signalling, if data file contains ' \
                                     'cscore.'
        if start_date is not None:
            self.start_date = parser.parse(start_date).timestamp()
        if end_date is not None:
            self.end_date = parser.parse(end_date).timestamp()

        slice_list = self.generate_slice_list(slice)

        # Create a SparkSession
        # Note: In case its run on Windows and generates errors use (tmp Folder mus exist):
        # spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("Postprocessing").getOrCreate()
        conf = SparkConf().setMaster("local[*]").setAppName("Test")
        sc = SparkContext(conf=conf)
        spark = SparkSession(sc).builder.appName("Calculate_Slices").getOrCreate()
        counter = 0
        snapshots = {}
        for file in self.events_files:
            counter = counter + 1
            events_source = spark.sparkContext.textFile(os.path.join(self.data_path, file))
            events = events_source.map(self.mapper_events)
            events_df = spark.createDataFrame(events).cache()
            if counter == 1:
                all_events_df = events_df
            else:
                all_events_df = all_events_df.union(events_df)
        for slice in slice_list:
            all_events_df.createOrReplaceTempView("all_events")
            active_edges_df = spark.sql('SELECT * FROM all_events WHERE revision < '+str(slice))
            active_edges_df.createOrReplaceTempView("events")
            active_edges_df = spark.sql('SELECT max(revision) as revision, source, target FROM events '
                                        'GROUP BY source, target')
            active_edges_df.createOrReplaceTempView("events")
            active_edges_df = spark.sql('SELECT e.revision, e.source, e.target, a.event FROM events e '
                                        'JOIN all_events a ON e.revision=a.revision AND e.source = a.source AND '
                                        'e.target = a.target')
            active_edges_df.createOrReplaceTempView("events")
            active_edges_df = spark.sql('SELECT source, target FROM events WHERE event = \'start\'')
            active_edges = active_edges_df.collect()
            snapshots[slice] = active_edges

        # TODO STORAGE OF RESULTS NEEDS TO BE DONE
        self.write_json('123file.json', snapshots)

    def sub_graph(self, seed=None, depth=3, include='cat'):
        assert include == 'cat' or consider == 'link' or consider == 'both', 'Error. Pass either cat, link or both for include'
        assert seed is not None, 'Error. One or more seed IDs need to be passed for creating a subgraph.'
        counter = 0
        for file in self.edges_files:
            counter = counter + 1
            edges_source = spark.sparkContext.textFile(os.path.join(self.data_path, file))
            edges = edges_source.map(self.mapper_events)
            edges_df = spark.createDataFrame(edges).cache()
            if counter == 1:
                all_edges_df = edges_df
            else:
                all_edges_df = all_edges_df.union(edges_df)

        seed = [item for sublist in seed for item in sublist]
        nodes = []
        nodes.append(seed)
        for i in range(depth):
            cond = self.assemble_condition(nodes, include)
            tmp_df = all_edges_df.where(cond)
            tmp_df.createOrReplaceTempView("tmp")
            tmp_results = spark.sql('SELECT source FROM tmp').distinct().rdd.collect()
            tmp_results = [item for sublist in tmp_results for item in sublist]
            nodes.append(tmp_results)


        self.write_list('file needs to be figured out', nodes) # destin
        # TODO DEALING WITH THE RESULTS

    def assemble_condition(self, seed, include):
        if include == 'cat':
            if type(seed) is list:
                for i in range(len(seed)):
                    if i == 0:
                        cond = col('target') == seed[i]
                    else:
                        cond = cond | col('target') == seed[i]
            else:
                cond = col('target') == seed
        elif include == 'link':
            if type(seed) is list:
                for i in range(len(seed)):
                    if i == 0:
                        cond = col('source') == seed[i]
                    else:
                        cond = cond | col('source') == seed[i]
            else:
                cond = col('source') == seed
        elif include == 'both':
            if type(seed) is list:
                for i in range(len(seed)):
                    if i == 0:
                        cond = col('target') == seed[i] | col('source') == seed[i]
                    else:
                        cond = cond | col('target') == seed[i] | col('source') == seed[i]
            else:
                cond = col('target') == seed | col('source') == seed
        return cond

    def sub_graph_views(self):
        # combination of temporal_views and sub_graph_views
        pass




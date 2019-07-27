import findspark
findspark.init()
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import collect_list, avg, max, stddev, mean
from wikiCat.processor.pandas_processor_graph import PandasProcessorGraph
from wikiCat.processor.spark_processor_graph import SparkProcessorGraph
from dateutil import parser
import math
#import datetime
import pandas as pd
import os


class ControvercyScore(PandasProcessorGraph, SparkProcessorGraph):
    def __init__(self, project):  # , fixed='fixed_none', errors='errors_removed'
        PandasProcessorGraph.__init__(self, project)
        SparkProcessorGraph.__init__(self, project)
        self.growth_rate = 1
        self.decay_rate = 0.0000001
        self.start_score = -0.9

    def set_constants(self, growth_rate=1, decay_rate=0.0000001, start_score=0):
        self.growth_rate = growth_rate
        self.decay_rate = decay_rate
        self.start_score = start_score

    def process_spark_list(self, row):
        source = row[0]
        target = row[1]
        ts_list = row[2]
        ts_list.sort()
        results = []

        for i in range(len(ts_list)):
            if i == 0:
                cscore = self.growth_rate
            else:
                delta = ts_list[i] - ts_list[i-1]
                cscore = cscore * math.exp(-1 * self.decay_rate * delta) + self.growth_rate
            results.append([ts_list[i], source, target, cscore])
        return results

    def calculate(self, scope):
        events = False
        nodes = False
        edges = False
        if scope == "all":
            events = True
            nodes = True
            edges = True
        if scope == "events":
            events = True
        if scope == "edges":
            events = True
        if scope == "nodes":
            nodes = True

        if events:
            print('Calculating cscores for events started.')
            self.calculate_event_score()
        if edges:
            print('Calculating cscores for edges started.')
            self.calculate_avg_edge_score()
        if nodes:
            print('Calculating cscores for nodes started.')
            self.calculate_avg_node_score()

    def calculate_event_score(self):
        print('============================')
        print('Calculate cScores for Events')
        print('============================')
        # Create a SparkSession
        # Note: In case its run on Windows and generates errors use (tmp Folder mus exist):

        spark = SparkSession\
            .builder\
            .appName("Calculate_Controvercy_Score_Edges")\
            .config("spark.driver.memory", "80g")\
            .config("spark.driver.maxResultSize", "80g")\
            .getOrCreate()


        print(SparkConf().getAll())

        for file in self.events_files:
            results_file = os.path.join(self.data_path, file)
            tmp_results_file = os.path.join(self.data_path, 'tmp_' + file)
            spark_results_path = os.path.join(self.data_path, file[:-4])

            events_source = spark.sparkContext.textFile(os.path.join(self.data_path, file))
            events = events_source.map(self.mapper_events)
            events_df = spark.createDataFrame(events).cache()
            events_df.createOrReplaceTempView("events")

            #Wahrscheinlich überflüssig:
            #events_grouped = events_df.drop('author').collect()
            #events_grouped_df = spark.createDataFrame(events_grouped).cache()
            #events_grouped_df.createOrReplaceTempView("events_grouped")
            #events_grouped_df = events_grouped_df.groupBy('source', 'target').agg(collect_list('revision').alias('revision'))

            events_grouped_df = events_df.groupBy('source', 'target').agg(collect_list('revision').alias('revision'))
            #events_grouped_df.show()
            initial_events = events_df.count()
            print("Number of events to be processed:", str(initial_events))

            cscore_events = events_grouped_df.rdd.map(self.process_spark_list)
            cscore_events = cscore_events.collect()

            # Processing list of cscore events and writing them to tmp file
            cscore_events = [item for sublist in cscore_events for item in sublist]

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

            # resolved_event_type_df.show()
            resolved_event_type_df.write.format('com.databricks.spark.csv').option('header', 'false').option('delimiter', '\t').save(spark_results_path)
            os.remove(tmp_results_file)
            self.assemble_spark_results(spark_results_path, tmp_results_file)
            os.remove(os.path.join(self.data_path, file))
            os.rename(tmp_results_file, results_file)

            processed_events = resolved_event_type_df.count()
            print('Number of processed and resolved events: ', str(processed_events))
            print("Status: ", initial_events == processed_events, '\n')

        del spark

    def calculate_avg_node_score(self):
        print('===================================')
        print('Calculate average cScores for Nodes')
        print('===================================')
        # TODO Handling of multiple Events files is probably not very performant
        # Create a SparkSession
        # Note: In case its run on Windows and generates errors use (tmp Folder mus exist):
        # spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").
        # appName("Postprocessing").getOrCreate()
        #spark = SparkSession.builder.appName("Calculate_Controvercy_Score_Nodes").getOrCreate()

        spark = SparkSession \
            .builder \
            .appName("Calculate_Controvercy_Score_Edges") \
            .config("spark.driver.memory", "80g") \
            .config("spark.driver.maxResultSize", "80g") \
            .getOrCreate()


        nodes_source = spark.sparkContext.textFile(os.path.join(os.getcwd(), self.data_path, self.nodes_files[0]))
        nodes = nodes_source.map(self.mapper_nodes)
        nodes_df = spark.createDataFrame(nodes).cache()
        nodes_df.createOrReplaceTempView("nodes")

        initial_nodes = nodes_df.count()
        print("Number of nodes to be processed:", str(initial_nodes))

        results_file = os.path.join(self.data_path, self.nodes_files[0])
        tmp_results_file = os.path.join(self.data_path, 'tmp_' + self.nodes_files[0])
        spark_results_path = os.path.join(self.data_path, self.nodes_files[0][:-4])

        init = True
        for file in self.events_files:
            events_source = spark.sparkContext.textFile(os.path.join(self.data_path, file))
            events = events_source.map(self.mapper_events)
            events_df = spark.createDataFrame(events).cache()
            events_df.createOrReplaceTempView("events")
            source_df = spark.sql('SELECT source as node, cscore FROM events')
            target_df = spark.sql('SELECT target as node, cscore FROM events')
            node_cscores_df = source_df.union(target_df)
            if init:
                init = False
                node_cscores_all = node_cscores_df
            else:
                node_cscores_all = node_cscores_all.union(node_cscores_df)



        avg_node_cscores_df = node_cscores_all.groupby('node').agg(avg('cscore').alias('avg_cscore'), mean('cscore').alias('mean_cscore'), max('cscore').alias('max_cscore'), stddev('cscore').alias('stddev_cscore'))
        nodes = avg_node_cscores_df

        #avg_node_cscores_df = node_cscores_all.groupby('node').agg(avg('cscore').alias('avg_cscore'))
        #avg_node_cscores_df.createOrReplaceTempView("cscore_nodes")
        #nodes = spark.sql("SELECT n.id, n.title, n.ns, c.avg_cscore as cscore "
        #                  "FROM nodes n LEFT OUTER JOIN cscore_nodes c ON n.id = c.node")

        nodes.write.format('com.databricks.spark.csv').option('header', 'false').option('delimiter', '\t').save(spark_results_path)

        self.assemble_spark_results(spark_results_path, tmp_results_file)
        os.remove(os.path.join(self.data_path, self.nodes_files[0]))
        os.rename(tmp_results_file, results_file)

        processed_nodes = nodes.count()
        print('Number of processed and resolved nodes: ', str(processed_nodes))
        print("Status: ", initial_nodes == processed_nodes, '\n')

        '''
        print('results assembled')
        # HANDLE Null Values in CSCORE: Replace NULL WITH ZERO Option 1
        print('Handle Cscore Null Values for Nodes')
        nodes = pd.read_csv(results_file, header=None, delimiter='\t',
                            names=['id', 'title', 'ns', 'cscore'], skip_blank_lines=True, na_filter=False,
                            error_bad_lines=False, warn_bad_lines=True)
        print('Number of nodes without cscore')
        print(len(nodes.loc[nodes['cscore'] == ""]))
        nodes.loc[nodes['cscore'] == "", 'cscore'] = 0.0
        nodes.to_csv(results_file, sep='\t', index=False, header=False, mode='w')
        '''
        del spark

    def calculate_avg_edge_score(self):
        print('===================================')
        print('Calculate average cScores for Edges')
        print('===================================')
        # TODO Handling of multiple Events files is probably not very performant
        # Create a SparkSession
        # Note: In case its run on Windows and generates errors use (tmp Folder mus exist):
        # spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("Postprocessing").getOrCreate()
        #spark = SparkSession.builder.appName("Calculate_Controvercy_Score_Edges").getOrCreate()

        spark = SparkSession\
            .builder\
            .appName("Calculate_Controvercy_Score_Edges")\
            .config("spark.driver.memory", "80g")\
            .config("spark.driver.maxResultSize", "80g")\
            .getOrCreate()

        edges_source = spark.sparkContext.textFile(os.path.join(self.data_path, self.edges_files[0]))
        edges = edges_source.map(self.mapper_edges)
        edges_df = spark.createDataFrame(edges).cache()
        edges_df.createOrReplaceTempView("edges")

        initial_edges = edges_df.count()
        print("Number of edges to be processed:", str(initial_edges))

        results_file = os.path.join(self.data_path, self.edges_files[0])
        tmp_results_file = os.path.join(self.data_path, 'tmp_' + self.edges_files[0])
        spark_results_path = os.path.join(self.data_path, self.edges_files[0][:-4])

        init = True
        for file in self.events_files:
            events_source = spark.sparkContext.textFile(os.path.join(self.data_path, file))
            events = events_source.map(self.mapper_events)
            events_df = spark.createDataFrame(events).cache()
            events_df.createOrReplaceTempView("events")
            if init:
                init=False
                events_all = events_df
            else:
                events_all = events_all.union(events_df)


        avg_edge_cscores_df = events_all.groupby('source', 'target').agg(avg('cscore').alias('avg_cscore'), mean('cscore').alias('mean_cscore'), max('cscore').alias('max_cscore'), stddev('cscore').alias('stddev_cscore'))
        edges = avg_edge_cscores_df

        #avg_edge_cscores_df = events_all.groupby('source', 'target').agg(avg('cscore').alias('avg_cscore'))
        #avg_edge_cscores_df.createOrReplaceTempView("cscore_edges")
        #edges = spark.sql("SELECT e.source, e.target, e.etype, c.avg_cscore as cscore "
        #                  "FROM edges e LEFT OUTER JOIN cscore_edges c "
        #                  "ON e.source = c.source AND e.target = c.target")

        edges.write.format('com.databricks.spark.csv').option('header', 'false').option('delimiter', '\t').save(spark_results_path)

        self.assemble_spark_results(spark_results_path, tmp_results_file)

        os.remove(os.path.join(self.data_path, self.edges_files[0]))
        os.rename(tmp_results_file, results_file)

        processed_edges = edges.count()
        print('Number of processed and resolved edges: ', str(processed_edges))
        print("Status: ", initial_edges == processed_edges, '\n')

        '''
        # HANDLE Null Values in CSCORE: Replace NULL WITH ZERO
        print('Handle Cscore Null Values for edges.')
        edges = pd.read_csv(results_file, header=None, delimiter='\t',
                            names=['source', 'target', 'type', 'cscore'], skip_blank_lines=True, na_filter=False,
                            error_bad_lines=False, warn_bad_lines=True)
        print('Number of edges without cscore')
        print(len(edges[edges['cscore'] == '']))
        edges.loc[edges['cscore'] == '', 'cscore'] = 0.0
        edges.to_csv(results_file, sep='\t', index=False, header=False, mode='w')
        '''
        del spark
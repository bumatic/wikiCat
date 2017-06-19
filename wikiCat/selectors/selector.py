import findspark
findspark.init()
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import SparkConf, SparkContext
from wikiCat.processors.spark_processor import SparkProcessorGraph
from dateutil import parser
from datetime import datetime
import pandas as pd
import os

#from wikiCat.data.wikigraph import WikiGraph
#import math
#from pyspark.sql.functions import collect_list, avg, col
#from wikiCat.processors.pandas_processor_graph import PandasProcessorGraph


class Selector(SparkProcessorGraph): #PandasProcessorGraph
    def __init__(self, graph):
        self.graph = graph
        self.data = self.graph.data
        assert self.graph.curr_working_graph is not None, 'Error. Set a current working graph before creating ' \
                                                          'a selector.'

        self.project = graph.project
        assert self.project.start_date is not None, 'Error. The project has no start date. Please generate it with ' \
                                                    'wikiCat.wikiproject.Project.find_start_date()'
        assert self.project.dump_date is not None, 'Error. The project has no start date. Please set it with ' \
                                                   'wikiCat.wikiproject.Project.set_dump_date(date)'

        SparkProcessorGraph.__init__(self, self.project)
        self.graph_id = self.graph.curr_working_graph
        self.working_graph = self.graph.curr_working_graph
        self.working_graph_path = self.graph.data[self.working_graph]['location']
        self.graph_path = self.graph.curr_data_path
        self.source_location = self.graph.source_location
        self.base_path = os.path.split(self.graph_path)[0]
        self.start_date = self.project.start_date.timestamp()
        self.end_date = self.project.dump_date.timestamp()
        self.results = {}
        self.gt_wiki_id_map_path, self.gt_wiki_id_map_file = self.find_gt_wiki_id_map()


    def find_gt_wiki_id_map(self):
        if 'gt_wiki_id_map' in self.data[self.working_graph].keys():
            file = self.data[self.working_graph]['gt_wiki_id_map']
            path = self.working_graph_path
        else:
            print('LOAD SUPER MAP')
            file = self.data['main']['gt_wiki_id_map']
            path = self.data['main']['location']
        return path, file


    def load_pandas_df(self, file, columns):
        df = pd.read_csv(file, header=None, delimiter='\t', names=columns)
        return df

    def check_results_path(self, folder):
        if not os.path.isdir(folder):
            os.makedirs(folder)
            return None
        elif not os.listdir(folder) == []:
            print('Non-empty directory for the Selector with this title already exists. Either delete the directory or use a different name.')
            return True

    def set_selector_dates(self, start_date, end_date):
        if start_date is not None:
            assert parser.parse(start_date).timestamp() >= self.start_date, 'Error. The start date needs to be after ' \
                                                                        + str(datetime.fromtimestamp(self.start_date))
            self.start_date = parser.parse(start_date).timestamp()

        if end_date is not None:
            assert parser.parse(end_date).timestamp() <= self.end_date, 'Error. The end date needs to be before ' \
                                                                        + str(datetime.fromtimestamp(self.end_date))
            self.end_date = parser.parse(end_date).timestamp()

    def create(self):
        pass


class Snapshots(Selector):
    def __init__(self, graph):
        Selector.__init__(self, graph)

    def create(self, title, slice='year', cscore=True, start_date=None, end_date=None):
        # TODO CHECK IF THIS WORKS!

        assert slice is 'year' or 'month' or 'day', 'Error. Pass a valid value for slice: year, month, day.'
        assert type(cscore) is bool, 'Error. A bool value is expected for cscore signalling, if data file contains ' \
                                     'cscore.'

        interval = slice
        self.set_selector_dates(start_date, end_date)
        slice_list = self.generate_snapshot_list(slice)
        snapshot_path = os.path.join(self.graph_path, title)
        conflict = self.check_results_path(snapshot_path)
        if conflict:
            return

        # Create a SparkSession
        # Note: In case its run on Windows and generates errors use (tmp Folder mus exist):
        # spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("Postprocessing").getOrCreate()
        conf = SparkConf().setMaster("local[*]").setAppName("Snapshots")
        sc = SparkContext(conf=conf)
        spark = SparkSession(sc).builder.appName("Calculate_Snapshots").getOrCreate()

        # Register dataframe for events data
        all_events_df = None
        for file in self.graph.source_events:
            events_source = spark.sparkContext.textFile(os.path.join(self.graph.source_events_location, file))
            events = events_source.map(self.mapper_events)
            events_df = spark.createDataFrame(events).cache()
            if all_events_df == None:
                all_events_df = events_df
            else:
                all_events_df = all_events_df.union(events_df)

        # Register dataframe for wiki_id to gt_id map
        id_map_source = spark.sparkContext.textFile(os.path.join(self.gt_wiki_id_map_path, self.gt_wiki_id_map_file))
        id_map = id_map_source.map(self.mapper_ids)
        id_map_df = spark.createDataFrame(id_map).cache()
        id_map_df.createOrReplaceTempView("id_map")

        # Calculate snapshots and store tmp results
        results_files = []
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
            active_edges_df.createOrReplaceTempView("edges")

            active_edges_df = spark.sql('SELECT i.gt_id as gt_source, e.target FROM edges e JOIN id_map i '
                                        'ON e.source = i.wiki_id')
            active_edges_df.createOrReplaceTempView("edges")
            active_edges_df = spark.sql('SELECT e.gt_source, i.gt_id as gt_target FROM edges e JOIN id_map i '
                                        'ON e.target = i.wiki_id')

            active_edges = active_edges_df.collect()
            results_file = str(slice) + '.csv'
            self.write_list(os.path.join(snapshot_path, results_file), active_edges)
            results_files.append(results_file)

        self.results['files'] = results_files
        self.results['type'] = 'snapshot'
        self.results['interval'] = interval
        self.results['start'] = str(datetime.fromtimestamp(self.start_date))
        self.results['end'] = str(datetime.fromtimestamp(self.end_date))

        self.data[self.graph_id][title] = self.results
        self.graph.update_graph_data(self.data)

        sc.stop()

    def generate_snapshot_list(self, slice):
        if slice == 'day':
            delta = 86400
        elif slice == 'month':
            delta = 2592000
        elif slice == 'year':
            delta = 31536000
        results = []
        curr_date = self.start_date + 86400
        while curr_date < self.end_date:
            results.append(curr_date)
            curr_date = curr_date + delta
        results.append(self.end_date)
        return results


class SubGraph(Selector):
    def __init__(self, graph):
        Selector.__init__(self, graph)

    def create(self, title=None, seed=None, cats=True, subcats=2, supercats=2, links=False, inlinks=2, outlinks=2):

        assert title is not None, 'Error. You need to pass a title of the subgraph.'
        #TODO adapt assertions
        #assert include == 'cat' or include == 'link' or include == 'both', 'Error. Pass either cat, link or both for include'
        assert seed is not None, 'Error. One or more seed IDs need to be passed for creating a sub graph.'
        assert type(seed) is list, 'Error. The seeds need to be passed as a list.'

        if title in self.data.keys():
            print('Sub graph with this title already exists. Choose another title or remove the sub graph first.')
            return False
        # Create a SparkSession
        # Note: In case its run on Windows and generates errors use (tmp Folder mus exist):
        # spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("Postprocessing").getOrCreate()
        conf = SparkConf().setMaster("local[*]").setAppName("Subgraph")
        sc = SparkContext(conf=conf)
        spark = SparkSession(sc).builder.appName("Create SubGraph").getOrCreate()

        # Register dataframe for edges
        for i in range(len(self.graph.source_edges)):
            edges_source = spark.sparkContext.textFile(os.path.join(self.graph.source_edges_location, self.graph.source_edges[i]))
            edges = edges_source.map(self.mapper_edges)
            edges_df = spark.createDataFrame(edges).cache()
            if i == 0:
                all_edges_df = edges_df
            else:
                all_edges_df = all_edges_df.union(edges_df)

        # Process edges and generate edge_results_df
        edge_results_df = None
        if cats:
            self.results['cats'] = {}
            nodes = seed
            cat_edges_df = all_edges_df.where(all_edges_df.etype == 'cat')
            if subcats is not None:
                self.results['cats']['subcats'] = subcats
                for i in range(subcats):
                    print('subcats iteration ' + str(i+1))
                    tmp_results = cat_edges_df[cat_edges_df.target.isin(nodes)]
                    if edge_results_df is None:
                        edge_results_df = tmp_results
                    else:
                        edge_results_df = edge_results_df.union(tmp_results).distinct()
                    print('Collect and process new seed nodes: ' + str(tmp_results.select(col('source')).distinct().count()))
                    try:
                        #TODO Implement error handling for end of tree in the other selects as well.
                        if tmp_results.select(col('source')).distinct().count() > 0:
                            tmp_nodes = tmp_results.select(col('source')).distinct().rdd.collect()
                            tmp_nodes = [item for sublist in tmp_nodes for item in sublist]
                            nodes = tmp_nodes
                            nodes = [str(i) for i in nodes] #cast items as str. otherwise results array does not work for spark
                    except:
                        print('failed')
                        print(tmp_results.select(col('source')).distinct().count())
            if supercats is not None:
                self.results['cats']['supercats'] = supercats
                for i in range(supercats):
                    print('supercats iteration ' + str(i+1))
                    tmp_results_source = cat_edges_df[cat_edges_df.source.isin(nodes)]
                    #Todo including members of supercategories results in very large graphs. Maybe there is anothter way of integrating those. For now I take it out.
                    tmp_results = tmp_results_source
                    '''
                    if i == 0:
                        #For the first iteration only follow sources
                        tmp_results = tmp_results_source
                    else:
                        tmp_results_target = cat_edges_df[cat_edges_df.target.isin(nodes)]
                        tmp_results = tmp_results_source.union(tmp_results_target).distinct()
                    '''

                    if edge_results_df is None:
                        edge_results_df = tmp_results
                    else:
                        edge_results_df = edge_results_df.union(tmp_results).distinct()
                    print('Collect and process new seed nodes: ' + str(tmp_results.select(col('target')).distinct().count()))
                    if tmp_results_source.select(col('target')).distinct().count() > 0:
                        tmp_nodes = tmp_results_source.select(col('target')).distinct().rdd.collect()
                        tmp_nodes = [item for sublist in tmp_nodes for item in sublist]
                        nodes = tmp_nodes
                        nodes = [str(i) for i in nodes] #cast items as str. otherwise results array does not work for spark
        if links:
            self.results['links'] = {}
            nodes = seed
            link_edges_df = all_edges_df.where(all_edges_df.etype == 'links')
            if inlinks is not None:
                self.results['links']['inlinks'] = inlinks
                for i in range(inlinks):
                    tmp_results = link_edges_df[link_edges_df.target.isin(nodes)]
                    tmp_nodes = tmp_results.select(col('source')).rdd.collect()
                    if edge_results_df is None:
                        edge_results_df = tmp_results
                    else:
                        edge_results_df = edge_results_df.union(tmp_results).distinct()
                    tmp_nodes = [item for sublist in tmp_nodes for item in sublist]
                    nodes = tmp_nodes
                    nodes = [str(i) for i in nodes] #cast items as str. otherwise results array does not work for spark
            if outlinks is not None:
                self.results['links']['outlinks'] = outlinks
                for i in range(outlinks):
                    tmp_results = link_edges_df[link_edges_df.source.isin(nodes)]
                    tmp_nodes = tmp_results.select(col('target')).rdd.collect()
                    if edge_results_df is None:
                        edge_results_df = tmp_results
                    else:
                        edge_results_df = edge_results_df.union(tmp_results).distinct()
                    tmp_nodes = [item for sublist in tmp_nodes for item in sublist]
                    nodes = tmp_nodes
                    nodes = [str(i) for i in nodes] #cast items as str. otherwise results array does not work for spark

        edge_results_df.createOrReplaceTempView("edge_results")
        edge_results_df = spark.sql('SELECT source, target, etype, cscore FROM edge_results').distinct()

        # Register events dataframe
        for i in range(len(self.graph.source_events)):
            events_source = spark.sparkContext.textFile(os.path.join(self.graph.source_events_location, self.graph.source_events[i]))
            events = events_source.map(self.mapper_events)
            events_df = spark.createDataFrame(events).cache()
            if i == 0:
                all_events_df = events_df
            else:
                all_events_df = all_events_df.union(events_df)

        print('Create events results')
        # Process events based on edge_results_df
        edge_results_df.createOrReplaceTempView("edge_results")
        all_events_df.createOrReplaceTempView("events")
        events_results_df = spark.sql('SELECT ev.revision, ev.source, ev.target, ev.event, ev.cscore '
                                      'FROM events ev JOIN edge_results ed ON ev.source = ed.source '
                                      'AND ev.target = ed.target')

        # Create results path and filenames
        results_path = os.path.join(self.base_path, str(title))
        self.check_results_path(results_path)
        edge_results_file = str(title) + '_' + self.graph.source_edges[0]
        events_results_file = str(title) + '_' + self.graph.source_events[0]

        # Collect results and write to file
        edge_results = edge_results_df.rdd.collect()
        self.write_list(os.path.join(results_path, edge_results_file), edge_results)
        events_results = events_results_df.rdd.collect()
        self.write_list(os.path.join(results_path, events_results_file), events_results)

        # Assemble results data and register to project
        self.results['source_edges'] = [edge_results_file]
        self.results['source_events'] = [events_results_file]
        self.results['location'] = results_path
        self.results['type'] = 'subgraph'
        self.results['derived_from'] = self.graph.curr_working_graph
        self.results['seeds'] = seed

        self.data[str(title)] = self.results
        self.graph.update_graph_data(self.data)

        # todo in every spark skript put sc.stop() at the end in order to enable chaining the processing steps.
        # without it one gets an error that only one sparkcontext can be created.
        sc.stop()
        print('end subgraph creation')
        return True


class GtEvents(Selector):
    def __init__(self, graph):
        Selector.__init__(self, graph)

    def create(self, title, cscore=True, start_date=None, end_date=None):
        assert type(cscore) is bool, 'Error. A bool value is expected for cscore signalling, if data file contains ' \
                                     'cscore.'
        self.set_selector_dates(start_date, end_date)

        # Create a SparkSession
        # Note: In case its run on Windows and generates errors use (tmp Folder mus exist):
        # spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("Postprocessing").getOrCreate()
        conf = SparkConf().setMaster("local[*]").setAppName("Events")
        sc = SparkContext(conf=conf)
        spark = SparkSession(sc).builder.appName("GtEvents").getOrCreate()

        # Create results path and filename
        results_path = os.path.join(self.graph.curr_data_path, str(title))
        print(results_path)
        self.check_results_path(results_path)
        events_results_file = str(title) + '_' + self.graph.source_events[0]

        # Register events dataframe
        for i in range(len(self.graph.source_events)):
            events_source = spark.sparkContext.textFile(
                os.path.join(self.graph.source_events_location, self.graph.source_events[i]))
            events = events_source.map(self.mapper_events)
            events_df = spark.createDataFrame(events).cache()
            if i == 0:
                all_events_df = events_df
            else:
                all_events_df = all_events_df.union(events_df)


        # Register dataframe for wiki_id to gt_id mapping
        id_map_source = spark.sparkContext.textFile(os.path.join(self.gt_wiki_id_map_path, self.gt_wiki_id_map_file))
        id_map = id_map_source.map(self.mapper_ids)
        id_map_df = spark.createDataFrame(id_map).cache()

        # Resolve wiki_ids to gt_id
        all_events_df.createOrReplaceTempView("events")
        id_map_df.createOrReplaceTempView("id_map")

        events_resolved_df = spark.sql('SELECT e.revision, i.gt_id as gt_source, e.target, e.event, e.cscore '
                                       'FROM events e LEFT OUTER JOIN id_map i ON e.source = i.wiki_id')
        events_resolved_df.createOrReplaceTempView("events_resolved")
        events_resolved_df = spark.sql('SELECT e.revision, e.gt_source, i.gt_id as gt_target, e.event, e.cscore '
                                       'FROM events_resolved e LEFT OUTER JOIN id_map i ON e.target = i.wiki_id')

        # Collect results and write to file
        events_resolved = events_resolved_df.rdd.collect()
        self.write_list(os.path.join(results_path, events_results_file), events_resolved)

        self.results['files'] = [events_results_file]
        self.results['type'] = 'gt_events'
        self.results['start'] = str(datetime.fromtimestamp(self.start_date))
        self.results['end'] = str(datetime.fromtimestamp(self.end_date))

        self.data[self.graph_id][title] = self.results
        self.graph.update_graph_data(self.data)

        sc.stop()


class SubGraphViews:
    def __init__(self, graph):
        self.graph = graph
        #Snapshots.__init__(self, graph)
        #SubGraph.__init__(self, graph)

    def create(self, subgraph_title, snapshot_title, seed=None, cats=True, subcats=2, supercats=2, links=False,
               inlinks=2, outlinks=2, slice='year', cscore=True, start_date=None, end_date=None):

        print('create subgraph')
        success = SubGraph(self.graph).create(title=subgraph_title, seed=seed, cats=cats, subcats=subcats,
                                              supercats=supercats, links=links, inlinks=inlinks, outlinks=outlinks)

        if not success:
            return
        else:
            print('SUCCEDSS')

        self.graph.set_working_graph(key=subgraph_title)
        print('create snapshots')
        Snapshots(self.graph).create(snapshot_title, slice=slice, cscore=cscore, start_date=start_date, end_date=end_date)





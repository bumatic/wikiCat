import os
from dateutil import parser
from datetime import datetime
from wikiCat.selector.selector import Selector, SparkProcessorGraph
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import col


class GraphSelector(SparkProcessorGraph): #PandasProcessorGraph
    def __init__(self, project):
        self.project = project
        self.graph = self.project.graph
        self.data = self.project.pinfo['data']['graph']
        assert 'start_date' in self.project.pinfo.keys(), 'Error. The project has no start date. Please generate' \
                                                          'it with wikiCat.wikiproject.Project.find_start_date()'
        assert 'dump_date' in self.project.pinfo.keys() is not None, 'Error. The project has no dump date. Please set ' \
                                                                     'it with wikiCat.wikiproject.Project.set_' \
                                                                     'dump_date(date)'

        SparkProcessorGraph.__init__(self, self.project)
        self.graph_path = self.project.pinfo['path']['graph']
        self.source_location = self.project.pinfo['path']['graph']
        self.base_path = os.path.split(self.graph_path)[0]
        self.start_date = parser.parse(self.project.pinfo['start_date']).timestamp()
        self.end_date = parser.parse(self.project.pinfo['dump_date']).timestamp()
        self.results = {}

    def check_results_path(self, folder):
        if not os.path.isdir(folder):
            os.makedirs(folder)
            return None
        elif not os.listdir(folder) == []:
            print(
                'Non-empty directory for the Selector with this title already exists. Either delete the directory or use a different name.')
            return True


class SeparateSubGraph(GraphSelector):
    def __init__(self, project):
        GraphSelector.__init__(self, project)

    def create(self, title=None, seed=None, cats=True, subcats=2, supercats=0, links=False, inlinks=2, outlinks=2):
        results_path = os.path.join(str(title))
        if self.check_results_path(results_path):
            return False

        assert title is not None, 'Error. You need to pass a title of the subgraph.'
        #TODO adapt assertions
        assert seed is not None, 'Error. One or more seed IDs need to be passed for creating a sub graph.'
        assert type(seed) is list, 'Error. The seeds need to be passed as a list.'

        if title in self.data.keys():
            print('Sub graph with this title already exists. Choose another title or remove the sub graph first.')
            return False
        # Create a SparkSession
        # Note: In case its run on Windows and generates errors use (tmp Folder mus exist):
        # spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("Postprocessing").getOrCreate()

        spark = SparkSession \
            .builder \
            .appName("Generate_Separate_Subgraph_Data") \
            .config("spark.driver.memory", "60g") \
            .config("spark.driver.maxResultSize", "60g") \
            .getOrCreate()

        print(SparkConf().getAll())



        # Register dataframe for edges
        for i in range(len(self.data['edges'])):
            edges_source = spark.sparkContext.textFile(os.path.join(self.graph_path, self.data['edges'][i]))
            edges = edges_source.map(self.mapper_edges)
            edges_df = spark.createDataFrame(edges).cache()
            if i == 0:
                all_edges_df = edges_df
            else:
                all_edges_df = all_edges_df.union(edges_df)

        #all_edges_df.show()

        # Process edges and generate edge_results_df
        edge_results_df = None
        result_nodes = seed
        if cats:
            self.results['cats'] = {}
            nodes = seed
            print('nodes')
            print(nodes)
            cat_edges_df = all_edges_df.where(all_edges_df.etype == 'cats')
            print('cat edges')
            #cat_edges_df.show()
            if subcats is not None:
                self.results['cats']['subcats'] = subcats
                for i in range(subcats):
                    print('subcats iteration ' + str(i+1))
                    tmp_results = cat_edges_df[cat_edges_df.target.isin(nodes)]
                    #tmp_results.show()

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
                            result_nodes = list(set(result_nodes + tmp_nodes))
                            nodes = tmp_nodes
                            nodes = [str(i) for i in nodes] #cast items as str. otherwise results array does not work for spark
                            seed = seed + list(set(nodes))
                            #print(nodes)
                            #print(seed)
                    except:
                        print('failed')
                        print(tmp_results.select(col('source')).distinct().count())
            if supercats is not None:
                nodes = seed
                self.results['cats']['supercats'] = supercats
                for i in range(supercats):
                    print('supercats iteration ' + str(i+1))
                    tmp_results_source = cat_edges_df[cat_edges_df.source.isin(nodes)]
                    #Todo including members of supercategories results in very large graphs. Maybe there is anothter way of integrating those. For now I take it out.
                    #tmp_results = tmp_results_source

                    if i == 0:
                        #For the first iteration only follow sources
                        tmp_results = tmp_results_source
                    else:
                        tmp_results_target = cat_edges_df[cat_edges_df.target.isin(nodes)]
                        tmp_results = tmp_results_source.union(tmp_results_target).distinct()

                    if edge_results_df is None:
                        edge_results_df = tmp_results
                    else:
                        edge_results_df = edge_results_df.union(tmp_results).distinct()
                    print('Collect and process new seed nodes: ' + str(tmp_results.select(col('target')).distinct().count()))
                    try:
                        if tmp_results_source.select(col('target')).distinct().count() > 0:
                            tmp_nodes = tmp_results_source.select(col('target')).distinct().rdd.collect()
                            tmp_nodes = [item for sublist in tmp_nodes for item in sublist]
                            result_nodes = list(set(result_nodes + tmp_nodes))
                            nodes = tmp_nodes
                            nodes = [str(i) for i in nodes] #cast items as str. otherwise results array does not work for spark
                            seed = seed + list(set(nodes))
                            #print(nodes)
                            #print(seed)
                    except:
                        print('failed')
                        print(tmp_results.select(col('source')).distinct().count())
        if links:
            self.results['links'] = {}
            link_edges_df = all_edges_df.where(all_edges_df.etype == 'links')
            #link_edges_df.show()
            if inlinks is not None:
                nodes = seed
                self.results['links']['inlinks'] = inlinks
                for i in range(inlinks):
                    print('inlinks iteration ' + str(i + 1))
                    tmp_results = link_edges_df[link_edges_df.target.isin(nodes)]
                    #tmp_results.show()
                    tmp_nodes = tmp_results.select(col('source')).rdd.collect()
                    if edge_results_df is None:
                        edge_results_df = tmp_results
                    else:
                        edge_results_df = edge_results_df.union(tmp_results).distinct()
                    print('Collect and process new seed nodes: ' + str(tmp_results.select(col('target')).distinct().count()))
                    try:
                        if tmp_results_source.select(col('source')).distinct().count() > 0:
                            tmp_nodes = tmp_results_source.select(col('source')).distinct().rdd.collect()
                            tmp_nodes = [item for sublist in tmp_nodes for item in sublist]
                            result_nodes = list(set(result_nodes + tmp_nodes))
                            nodes = tmp_nodes
                            nodes = [str(i) for i in nodes] #cast items as str. otherwise results array does not work for spark
                            seed = seed + list(set(nodes))
                            #print(nodes)
                            #print(seed)
                    except:
                        print('failed')
                        print(tmp_results.select(col('source')).distinct().count())
            if outlinks is not None:
                nodes = seed
                self.results['links']['outlinks'] = outlinks
                for i in range(outlinks):
                    print('outlinks iteration ' + str(i + 1))
                    tmp_results = link_edges_df[link_edges_df.source.isin(nodes)]
                    #tmp_results.show()
                    tmp_nodes = tmp_results.select(col('target')).rdd.collect()
                    if edge_results_df is None:
                        edge_results_df = tmp_results
                    else:
                        edge_results_df = edge_results_df.union(tmp_results).distinct()
                    print('Collect and process new seed nodes: ' + str(tmp_results.select(col('target')).distinct().count()))
                    try:
                        if tmp_results_source.select(col('target')).distinct().count() > 0:
                            tmp_nodes = tmp_results_source.select(col('target')).distinct().rdd.collect()
                            tmp_nodes = [item for sublist in tmp_nodes for item in sublist]
                            result_nodes = list(set(result_nodes + tmp_nodes))
                            nodes = tmp_nodes
                            nodes = [str(i) for i in nodes] #cast items as str. otherwise results array does not work for spark
                            seed = seed + list(set(nodes))
                            #print(nodes)
                            #print(seed)
                    except:
                        print('failed')
                        print(tmp_results.select(col('source')).distinct().count())

        edge_results_df.createOrReplaceTempView("edge_results")
        #edge_results_df = spark.sql('SELECT source, target, etype, cscore FROM edge_results').distinct()
        edge_results_df = spark.sql('SELECT source, target, etype FROM edge_results').distinct()


        print('EDGE RESULTS DF')
        print('===============')
        edge_results_df.show()


        for i in range(len(self.data['events'])):
            events_source = spark.sparkContext.textFile(
                os.path.join(self.graph_path, self.data['events'][i]))


            events = events_source.map(self.mapper_events)

            events_df = spark.createDataFrame(events).cache()


            # Process events based on edge_results_df
            edge_results_df.createOrReplaceTempView("edge_results")
            events_df.createOrReplaceTempView("events")
            events_tmp_results_df = spark.sql('SELECT ev.revision, ev.source, ev.target, ev.event ' #, ev.cscore 
                                              'FROM events ev JOIN edge_results ed ON ev.source = ed.source '
                                              'AND ev.target = ed.target')
            print('TMP EVENT RESULTS DF FOR: ' + events_source)
            print('==========================================================')
            events_tmp_results_df.show()

            try:
                if i == 0:
                    events_results_df = events_tmp_results_df
                else:
                    events_results_df = events_results_df.union(events_tmp_results_df)

                #events_results_df.show()
            except:
                print('Failed for events source')
                print(events_source)
                events_tmp_results_df.show()

        # Create results path and filenames

        #results_path = os.path.join(self.base_path, str(title))

        edge_results_file = str(title) + '_edges.csv'
        events_results_file = str(title) + '_events.csv'

        # Collect results and write to file
        edge_results = edge_results_df.rdd.collect()
        self.write_list(os.path.join(results_path, edge_results_file), edge_results)
        events_results = events_results_df.rdd.collect()
        self.write_list(os.path.join(results_path, events_results_file), events_results)


        # todo in every spark script put sc.stop() at the end in order to enable chaining the processing steps.
        # without it one gets an error that only one sparkcontext can be created.
        spark.stop()
        print('Subgraph creation finished.')
        return True

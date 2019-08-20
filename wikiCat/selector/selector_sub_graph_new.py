import os
from wikiCat.selector.selector import Selector, SparkProcessorGraph
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import col


class GraphSelector(SparkProcessorGraph): #PandasProcessorGraph
    def __init__(self, project):
        #self.graph = graph
        #self.data = self.graph.data
        self.project = project
        self.graph = self.project.graph
        self.data = self.project.pinfo['data']['graph'] #self.project.pinfo['gt_graph']
        print(self.project.pinfo['data']['graph'])
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
        #self.gt_wiki_id_map_path, self.gt_wiki_id_map_file = self.find_gt_wiki_id_map()


class SeparateSubGraph(GraphSelector):
    def __init__(self, project):
        GraphSelector.__init__(self, project)

    def create(self, title=None, seed=None, cats=True, subcats=2, supercats=0, links=False, inlinks=2, outlinks=2):

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
            #nodes = seed ??????????
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


        '''
        #Todo: Working original. Needs to be reworked for large amount of event data.
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
        '''

        for i in range(len(self.graph.source_events)):
            events_source = spark.sparkContext.textFile(
                os.path.join(self.graph.source_events_location, self.graph.source_events[i]))
            events = events_source.map(self.mapper_events)
            events_df = spark.createDataFrame(events).cache()

            # Process events based on edge_results_df
            edge_results_df.createOrReplaceTempView("edge_results")
            events_df.createOrReplaceTempView("events")
            events_tmp_results_df = spark.sql('SELECT ev.revision, ev.source, ev.target, ev.event, ev.cscore '
                                              'FROM events ev JOIN edge_results ed ON ev.source = ed.source '
                                              'AND ev.target = ed.target')
            if i == 0:
                events_results_df = events_tmp_results_df
            else:
                events_results_df = events_results_df.union(events_tmp_results_df)



        # Create results path and filenames
        results_path = os.path.join(str(title))
        #results_path = os.path.join(self.base_path, str(title))
        self.check_results_path(results_path)
        edge_results_file = str(title) + '_edges.csv'
        events_results_file = str(title) + '_events.csv'

        # Collect results and write to file
        edge_results = edge_results_df.rdd.collect()
        self.write_list(os.path.join(results_path, edge_results_file), edge_results)
        events_results = events_results_df.rdd.collect()
        self.write_list(os.path.join(results_path, events_results_file), events_results)

        '''
        # Assemble results data and register to project
        self.results['source_edges'] = [edge_results_file]
        self.results['source_events'] = [events_results_file]
        self.results['location'] = results_path
        self.results['type'] = 'subgraph'
        self.results['derived_from'] = self.graph.curr_working_graph
        self.results['seeds'] = seed
        
        self.data[str(title)] = self.results
        self.graph.update_graph_data(self.data)
        '''
        
        # todo in every spark script put sc.stop() at the end in order to enable chaining the processing steps.
        # without it one gets an error that only one sparkcontext can be created.
        sc.stop()
        print('Subgraph creation finished.')
        return True

from wikiCat.processor.spark_processor_parsed import SparkProcessorParsed
import os
import pandas as pd
import findspark
import subprocess
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
# from pyspark.sql import Row
# from dateutil import parser
# import collections


# TODO For dealing with link data a different approach needs to be implemented. Idea outlined:
# First iterate over all files and split them into chunks of x (maybe 1000) source pages
# It needs to be taken care of that all date for one page ends up in just one file.
# Then calculate events for all files.
# store their destination in a tmp list
# iterate over all results and create a list of all edges and nodes
# generate nodes and edges results based on that list!


# Example call: GraphDataGenerator(myProject, 'cat_data').generate_graph_data()
class GraphDataGenerator(SparkProcessorParsed):
    def __init__(self, project):  # , link_data_type, fixed='fixed_none', errors='errors_removed'
        SparkProcessorParsed.__init__(self, project)

    def get_page_data(self, link_data_type):
        if type(self.project.pinfo['data']['parsed'][link_data_type]) == list:
            page_data = self.project.pinfo['data']['parsed'][link_data_type]
        elif type(self.project.pinfo['data']['parsed'][link_data_type]) == str:
            page_data = [self.project.pinfo['data']['parsed'][link_data_type]]
        return page_data

    def generate_graph_data(self, create, override=False):
        if 'graph' in self.project.pinfo['data'].keys() and not override:
            print('Graph Data already exists in project. Pass override to replace it.')
            return False
        elif 'graph' in self.project.pinfo['data'].keys() and override:
            self.remove_old('graph')

        results = {}

        if create == 'cats':
            #link_data_type = 'cats'
            edge_type = 'cat'
            results_basename = 'cats'
            page_data = self.get_page_data('cat_data')
            results['cats'] = self.generate(edge_type, results_basename, page_data, 'cats')
            #print(results)
        elif create == 'links':
            #link_data_type = 'links'
            edge_type = 'links'
            results_basename = 'links'
            page_data = self.get_page_data('link_data')
            results['links'] = self.generate(edge_type, results_basename, page_data, 'links')
            pass
        elif create == 'all':
            #link_data_type = 'cats'
            edge_type = 'cat'
            results_basename = 'cats'
            page_data = self.get_page_data('cat_data')
            results['cats'] = self.generate(edge_type, results_basename, page_data, 'cats')

            edge_type = 'links'
            results_basename = 'links'
            page_data = self.get_page_data('link_data')
            results['links'] = self.generate(edge_type, results_basename, page_data, 'links')

        self.handle_results(results)

        results = {
            'nodes': 'nodes.csv',
            'edges': 'edges.csv',
            'events': 'events.csv',
            'description': 'Graph data created from parsed data.'
        }

        self.register_graph_results('graph', results)

    def handle_results(self, results):
        for key, value in results.items():
            self.append_data(value['nodes'], 'nodes.csv')
            self.append_data(value['edges'], 'edges.csv')
            self.append_data(value['events'], 'events.csv')

    def append_data(self, src_files, dest_file):
        dest_file = os.path.join(self.project.pinfo['path']['graph'], dest_file)
        for src_file in src_files:
            src_file = os.path.join(self.project.pinfo['path']['graph'], src_file)
            data_new = pd.read_csv(src_file, header=None, delimiter='\t', na_filter=False)
            data_new.to_csv(dest_file, sep='\t', index=False, header=False, mode='a')
            os.remove(src_file)

    def generate(self, edge_type, results_basename, page_data, data_type):
        # Create a SparkSession
        # Note: In case its run on Windows and generates errors use (tmp Folder mus exist):
        # spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp")
        # .appName("Postprocessing").getOrCreate()
        spark = SparkSession.builder.appName("Generate_Graph_Data").getOrCreate()

        # Infer the schema, and register the DataFrames as tables.
        page_info_source = spark.sparkContext.textFile(os.path.join(self.data_path, self.page_info))
        page_info = page_info_source.map(self.mapper_page_info)
        page_info_df = spark.createDataFrame(page_info).cache()
        page_info_df.createOrReplaceTempView("info")

        # Infer the schema, and register the DataFrames as tables.
        author_info_source = spark.sparkContext.textFile(os.path.join(self.data_path, self.author_info))
        author_info = author_info_source.map(self.mapper_author_info)
        author_info_df = spark.createDataFrame(author_info).cache()

        missing_author_row = spark.createDataFrame([[-1, "NO_AUTHOR_DATA"]])
        author_info_df = author_info_df.union(missing_author_row)
        #display(appended)
        author_info_df.createOrReplaceTempView("author")



        # TODO Remove When Graph Data Generator Works
        # Create DF with only cat_info
        # needs to be replaced/removed when cat_data file has been fixed
        # cat_info_df = spark.sql("SELECT * FROM info WHERE page_ns=14")
        # cat_info_df.createOrReplaceTempView("cat_info")

        # Infer the schema, and register the DataFrames as tables.
        revision_info_source = spark.sparkContext.textFile(os.path.join(self.data_path, self.revision_info))
        revision_info = revision_info_source.map(self.mapper_revisions)
        revision_info_df = spark.createDataFrame(revision_info).cache()
        revision_info_df.createOrReplaceTempView("revision")

        resolved_authors_df = spark.sql(
            'SELECT r.rev_id, r.rev_date, a.author_name as rev_author '
            'FROM revision r LEFT OUTER JOIN author a ON r.rev_author = a.author_id')
        revision_info_df = resolved_authors_df
        print('revision')
        revision_info_df.show()
        revision_info_df.createOrReplaceTempView("revision")



        counter = 0

        edges_results = []
        nodes_results = []
        events_results = []

        for f in page_data:
            print(f)
            print('')
            print('')
            counter = counter + 1
            compressed = False
            if f[-2:] == '7z':
                compressed = True
                print(compressed)
                subprocess.call(['7z', 'e', os.path.join(self.data_path, f), '-o'+self.data_path])
                if data_type == 'cats':
                    f = 'cats.csv'
                elif data_type == 'links':
                    f = 'links.csv'

            # Results files
            edges_results_path = os.path.join(self.results_path, results_basename + '_' + str(counter) + '_edges/')
            edges_results_file = os.path.join(self.results_path, results_basename + '_' + str(counter) + '_edges.csv')
            nodes_results_path = os.path.join(self.results_path, results_basename + '_' + str(counter) + '_nodes/')
            nodes_results_file = os.path.join(self.results_path, results_basename + '_' + str(counter) + '_nodes.csv')
            events_results_path = os.path.join(self.results_path, results_basename + '_' + str(counter) + '_events/')
            events_results_file = os.path.join(self.results_path, results_basename + '_' + str(counter) + '_events.csv')

            # Infer the schema, and register the DataFrames as tables.
            page_data_source = spark.sparkContext.textFile(os.path.join(self.data_path, f))
            page_data = page_data_source.map(self.mapper_page_data)
            page_data_df = spark.createDataFrame(page_data).cache()
            page_data_df.createOrReplaceTempView("data")

            # 1. RESOLVE PAGE TITLES
            resolved_titles_df = spark.sql(
                'SELECT d.source_id as source, i.page_id as target, d.target_title, d.rev_id as revision '
                'FROM data d LEFT OUTER JOIN info i ON UPPER(d.target_title) = UPPER(i.page_title)')
            resolved_titles_df.createOrReplaceTempView('resolved')

            page_data_df = spark.sql('SELECT r.source, r.target, r.revision FROM resolved r WHERE r.target IS NOT NULL')
            page_data_df.createOrReplaceTempView('data')

            # 2. GENERATE, ADD EDGE TYPE AND SAVE EDGE LIST
            edges_df = spark.sql('SELECT source, target FROM data').distinct()
            edges_df = edges_df.withColumn('type', lit(edge_type))
            edges_df = edges_df
            edges_df.write.format('com.databricks.spark.csv').option('header', 'false').option('delimiter', '\t')\
                .save(edges_results_path)
            del edges_df
            self.assemble_spark_results(edges_results_path, edges_results_file)

            # 3. GENERATE AND SAVE NODE LIST

            page_info_df.select('page_id', 'page_title', 'page_ns').write.format('com.databricks.spark.csv').option('header', 'false').option('delimiter', '\t').save(nodes_results_path)
            self.assemble_spark_results(nodes_results_path, nodes_results_file)

            # 4.1 CREATE TABLE WITH ALL REVISIONS OF A SOURCE PAGE
            page_revisions_df = spark.sql('SELECT source, revision FROM data').distinct()

            #print('4.1 page revisions df')
            #page_revisions_df.show()
            page_revisions_df.createOrReplaceTempView('page_revisions')

            # 4.2 CREATE TABLE WITH ALL POSSIBLE COMBINATIONS of SOURCE & REV with TARGETS
            all_possibilities_df = spark.sql('SELECT p.source, d.target, p.revision '
                                             'FROM page_revisions p JOIN data d '
                                             'ON p.source = d.source').distinct()
            #print('4.2 all possibilities')
            #all_possibilities_df.show()

            # 5. CREATE TABLE WITH ENTRIES FOR WHEN A EDGE DID NOT EXIST AT THE TIME OF A REVISION
            # BY SUBTRACTING EXISTING EDGES FROM ALL_POSSIBILITIES
            negative_edges_df = all_possibilities_df.subtract(page_data_df).sort('source', 'revision')
            #print('5')
            #negative_edges_df.show()
            negative_edges_df.createOrReplaceTempView('negative_edges')

            # 6.1 FIRST STEP TO CREATE DURATION RESULTS BY MAKING A LEFT OUTER JOIN OF DATA WITH NEGATIVE EDGES
            # ON SOURCE = TARGET AND DATA.REVISION < NEGATIVE_EDGES.REVISION
            # INCLUDES ALL RESULTS FROM START TIMES TO POTENTIAL LATER END TIMES

            durations_df = spark.sql('SELECT d.source, d.target, d.revision as start, n.revision as end '
                                     'FROM data d LEFT OUTER JOIN negative_edges n '
                                     'ON d.source = n.source AND d.target = n.target AND d.revision < n.revision')
            #print('6.1')
            #durations_df.show()

            durations_df.createOrReplaceTempView("durations")

            # 6.2 SECOND STEP TO CREATE DURATION RESULTS BY KEEPING ONLY MIN END TIMES
            # FOR GROUPS OF SOURCE, TARGET, START
            durations_df = spark.sql('SELECT source, target, start, min(end) as end FROM durations '
                                     'GROUP BY source, target, start')
            #print('6.2')
            #durations_df.show()
            durations_df.createOrReplaceTempView("durations")

            # 6.3 THIRD STEP TO CREATE DURATION RESULTS BY KEEPING ONLY MIN START TIMES
            # FOR GROUPS OF SOURCE, TARGET, END
            durations_df = spark.sql('SELECT source, target, min(start) as start, end FROM durations '
                                     'GROUP BY source, target, end').distinct()
            #print('6.3')
            #durations_df.show()
            durations_df.createOrReplaceTempView("durations")

            # 7.1 CREATE START COLUMN
            durations_df = durations_df.withColumn('event_start', lit('start'))

            #print('print 7.1')
            #durations_df.show()
            durations_df.createOrReplaceTempView("durations")

            # 7.2 CREATE END COLUMN
            durations_df = durations_df.withColumn('event_end', lit('end'))
            #print('7.2')
            #durations_df.show()
            durations_df.createOrReplaceTempView("durations")

            # 8. CREATE TABLES FOR START EVENTS AND END EVENTS
            start_events_df = spark.sql('SELECT source, target, start as revision, event_start as event FROM durations')
            #print('8 - start, end')
            #start_events_df.show()
            start_events_df.createOrReplaceTempView("start_events")

            end_events_df = spark.sql('SELECT source, target, end as revision, event_end as event FROM durations')
            #end_events_df.show()
            end_events_df.createOrReplaceTempView("end_events")

            # 9. COMBINE THE TABLES START_EVENTS AND END_EVENTS, SORT BY REVISION
            events_df = start_events_df.union(end_events_df).distinct()
            print('9')
            events_df.show()
            events_df.createOrReplaceTempView("events")

            # 10. RESOLVE REVISION_ID TO TIMES, SORT BY TIME, AND STORE
            events_df = spark.sql('SELECT r.rev_date as revision, e.source, e.target, e.event, r.rev_author as author '
                                  'FROM events e JOIN revision r ON e.revision = r.rev_id').distinct().sort('revision')
            print('10')
            events_df.show()
            events_df.write.format('com.databricks.spark.csv').option('header', 'false').option('delimiter', '\t')\
                .save(events_results_path)
            del events_df
            self.assemble_spark_results(events_results_path, events_results_file)

            if compressed:
                os.remove(os.path.join(self.data_path, f))

            path, nodes_results_file = os.path.split(nodes_results_file)
            nodes_results.append(nodes_results_file)
            path, edges_results_file = os.path.split(edges_results_file)
            edges_results.append(edges_results_file)
            path, events_results_file = os.path.split(events_results_file)
            events_results.append(events_results_file)

        results = {
            'nodes': nodes_results,
            'edges': edges_results,
            'events': events_results
        }

        return results

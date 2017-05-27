from wikiCat.processors.spark_processor_parsed import SparkProcessorParsed
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
# from pyspark.sql import Row
# from dateutil import parser
# import collections
import os


class GraphDataGenerator(SparkProcessorParsed):
    def __init__(self, project, link_data_type, fixed='fixed_none', errors='errors_removed'):
        SparkProcessorParsed.__init__(self, project)
        self.link_data_type = link_data_type
        if self.link_data_type == 'cat_data':
            self.edge_type = 'cat'
        elif self.link_data_type == 'link_data':
            self.edge_type = 'link'
        self.fixed = fixed
        self.errors = errors
        self.results_basename = self.link_data_type+'_'+self.fixed+'_'+self.errors+'_'
        self.page_data = self.data_obj.data[self.link_data_type]

    def generate_graph_data(self, override=False):
        # Create a SparkSession
        # Note: In case its run on Windows and generates errors use (tmp Folder mus exist):
        # spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("Postprocessing").getOrCreate()
        spark = SparkSession.builder.appName("Generate_Graph_Data").getOrCreate()

        # Infer the schema, and register the DataFrames as tables.
        page_info_source = spark.sparkContext.textFile(os.path.join(self.data_path, self.page_info))
        page_info = page_info_source.map(self.mapper_page_info)
        page_info_df = spark.createDataFrame(page_info).cache()
        page_info_df.createOrReplaceTempView("info")

        # Infer the schema, and register the DataFrames as tables.
        revision_info_source = spark.sparkContext.textFile(os.path.join(self.data_path, self.revision_info))
        revision_info = revision_info_source.map(self.mapper_revisions)
        revision_info_df = spark.createDataFrame(revision_info).cache()
        revision_info_df.createOrReplaceTempView("revision")

        counter = 0

        edges_results = []
        nodes_results = []
        events_results = []

        for file in self.page_data:
            counter = counter + 1

            # Results files
            edges_results_path = os.path.join(self.results_path, self.results_basename + str(counter) + '_edges/')
            edges_results_file = os.path.join(self.results_path, self.results_basename + str(counter) + '_edges.csv')
            nodes_results_path = os.path.join(self.results_path, self.results_basename + str(counter) + '_nodes/')
            nodes_results_file = os.path.join(self.results_path, self.results_basename + str(counter) + '_nodes.csv')
            events_results_path = os.path.join(self.results_path, self.results_basename + str(counter) + '_events/')
            events_results_file = os.path.join(self.results_path, self.results_basename + str(counter) + '_events.csv')


            # Infer the schema, and register the DataFrames as tables.
            page_data_source = spark.sparkContext.textFile(os.path.join(self.data_path, file[0]))
            page_data = page_data_source.map(self.mapper_page_data)
            page_data_df = spark.createDataFrame(page_data).cache()
            page_data_df.createOrReplaceTempView("data")

            #self.data_file_basename = file[0][:-4]

            # 1. RESOLVE PAGE TITLES

            # Auflösung der Titel funktioniert gerade nur für Cats und nicht für links! GRUND: FEHLER beim Parsing/Processing von
            # Cat data > Hier müsste Category: am Titel dran bleiben. Wenn dies gelöst, dann oben im mapper der page info auch das removal von
            # "Category:" entfernen

            cat_info_df = spark.sql("SELECT * FROM info WHERE page_ns=14")
            cat_info_df.createOrReplaceTempView("cat_info")

            resolved_titles_df = spark.sql(
                'SELECT d.source_id as source, i.page_id as target, d.target_title, d.rev_id as revision '
                'FROM data d LEFT OUTER JOIN cat_info i ON UPPER(d.target_title) = UPPER(i.page_title)')

            # resolved_titles_df = spark.createDataFrame(resolved_titles).cache()
            resolved_titles_df.createOrReplaceTempView('resolved')

            page_data_df = spark.sql('SELECT r.source, r.target, r.revision FROM resolved r WHERE r.target IS NOT NULL')
            page_data_df.createOrReplaceTempView('data')

            # 2. GENERATE, ADD EDGE TYPE AND SAVE EDGE LIST
            edges_df = spark.sql('SELECT source, target FROM data').distinct()
            edges_df = edges_df.withColumn('type', lit(self.edge_type))
            #edges_results = edges_df.collect()
            #self.write_list(edges_results_file, edges_results)
            edges_df = edges_df #.coalesce(1)
            edges_df.write.format('com.databricks.spark.csv').option('header', 'false').option('delimiter', '\t')\
                .save(edges_results_path)
            del edges_df
            self.assemble_spark_results(edges_results_path, edges_results_file)

            # 3. GENERATE AND SAVE NODE LIST
            source_df = spark.sql("SELECT source as id FROM data").distinct()
            target_df = spark.sql("SELECT target as id FROM data").distinct()
            nodes_df = source_df.union(target_df).distinct()

            #nodes_df = spark.sql("SELECT CONCAT(source, target) as id FROM data").distinct()
            print('nodes count union: '+str(nodes_df.count()))
            nodes_df.createOrReplaceTempView('nodes')

            nodes_df = spark.sql('SELECT n.id, i.page_title, i.page_ns FROM nodes n LEFT OUTER JOIN info i ON n.id=i.page_id').distinct()
            print('nodes count joined: ' + str(nodes_df.count()))
            # nodes_results = nodes_df.collect()
            # self.write_list(nodes_results_file, nodes_results)
            # nodes_df = nodes_df #.coalesce(1)
            nodes_df.write.format('com.databricks.spark.csv').option('header', 'false').option('delimiter', '\t')\
                .save(nodes_results_path)
            del nodes_df
            self.assemble_spark_results(nodes_results_path, nodes_results_file)

            # 4. CREATE TABLE WITH ALL REVISIONS OF A SOURCE PAGE
            page_revisions_df = spark.sql('SELECT source, revision FROM data').distinct()
            page_revisions_df.createOrReplaceTempView('page_revisions')

            # 4. CREATE TABLE WITH ALL POSSIBLE COMBINATIONS of SOURCE & REV with TARGETS
            all_possibilities_df = spark.sql('SELECT p.source, p.revision, d.target '
                                             'FROM page_revisions p JOIN data d '
                                             'ON p.source = d.source').distinct()

            # 5. CREATE TABLE WITH ENTRIES FOR WHEN A EDGE DID NOT EXIST AT THE TIME OF A REVISION
            # BY SUBTRACTING EXISTING EDGES FROM ALL_POSSIBILITIES
            negative_edges_df = all_possibilities_df.subtract(page_data_df).sort('source', 'revision')
            negative_edges_df.createOrReplaceTempView('negative_edges')

            # 6.1 FIRST STEP TO CREATE DURATION RESULTS BY MAKING A LEFT OUTER JOIN OF DATA WITH NEGATIVE EDGES
            # ON SOURCE = TARGET AND DATA.REVISION < NEGATIVE_EDGES.REVISION
            # INCLUDES ALL RESULTS FROM START TIMES TO POTENTIAL LATER END TIMES

            durations_df = spark.sql('SELECT d.source, d.target, d.revision as start, n.revision as end '
                                     'FROM data d LEFT OUTER JOIN negative_edges n '
                                     'ON d.source = n.source AND d.target = n.target AND d.revision < n.revision')
            durations_df.createOrReplaceTempView("durations")

            # 6.2 SECOND STEP TO CREATE DURATION RESULTS BY KEEPING ONLY MIN END TIMES
            # FOR GROUPS OF SOURCE, TARGET, START
            durations_df = spark.sql('SELECT source, target, start, min(end) as end FROM durations '
                                     'GROUP BY source, target, start')
            durations_df.createOrReplaceTempView("durations")

            # 6.3 THIRD STEP TO CREATE DURATION RESULTS BY KEEPING ONLY MIN START TIMES
            # FOR GROUPS OF SOURCE, TARGET, END
            durations_df = spark.sql('SELECT source, target, min(start) as start, end FROM durations '
                                     'GROUP BY source, target, end')
            durations_df.createOrReplaceTempView("durations")

            # 7.1 CREATE START COLUMN
            durations_df = durations_df.withColumn('event_start', lit('start'))
            durations_df.createOrReplaceTempView("durations")

            # 7.2 CREATE END COLUMN
            durations_df = durations_df.withColumn('event_end', lit('end'))
            durations_df.createOrReplaceTempView("durations")

            # 8. CREATE TABLES FOR START EVENTS AND END EVENTS
            start_events_df = spark.sql('SELECT source, target, start as revision, event_start as event FROM durations')
            start_events_df.createOrReplaceTempView("start_events")

            end_events_df = spark.sql('SELECT source, target, end as revision, event_end as event FROM durations')
            end_events_df.createOrReplaceTempView("end_events")

            # 9. COMBINE THE TABLES START_EVENTS AND END_EVENTS, SORT BY REVISION

            events_df = start_events_df.union(end_events_df).distinct()
            events_df.createOrReplaceTempView("events")

            # 10. RESOLVE REVISION_ID TO TIMES, SORT BY TIME, AND STORE

            events_df = spark.sql('SELECT r.rev_date as revision, e.source, e.target, e.event '
                                  'FROM events e JOIN revision r ON e.revision = r.rev_id').distinct().sort('revision')
            # events_results = events_df.collect()
            # self.write_list(events_results_file, events_results)
            events_df = events_df #.coalesce(1)
            events_df.write.format('com.databricks.spark.csv').option('header', 'false').option('delimiter', '\t')\
                .save(events_results_path)
            del events_df
            self.assemble_spark_results(events_results_path, events_results_file)

            path, nodes_results_file = os.path.split(nodes_results_file)
            nodes_results.append(nodes_results_file)
            path, edges_results_file = os.path.split(edges_results_file)
            edges_results.append(edges_results_file)
            path, events_results_file = os.path.split(events_results_file)
            events_results.append(events_results_file)



        self.register_results('graph', nodes=nodes_results, edges=edges_results, events=events_results,
                              fixed=self.fixed, errors=self.errors, override=override)
        return





from wikiCat.processor.spark_processor_parsed import SparkProcessorParsed
import findspark
findspark.init()
from pyspark.sql import SparkSession
import os


class ParsedDataErrorCheck(SparkProcessorParsed):
    def __init__(self, project, link_data_type):
        SparkProcessorParsed.__init__(self, project)
        self.project = project
        self.link_data_type = link_data_type
        self.page_data = self.project.pinfo['data']['parsed'][self.link_data_type]
        self.error_path = self.project.pinfo['path']['errors']
        self.error_basename = 'error_' + link_data_type + '_'
        self.data_file_basename = ''

    def missing_info_source_ids(self):
        self.error_basename = 'info_missing_for_source_ids_'
        # Create a SparkSession
        # Note: In case its run on Windows and generates errors use (tmp Folder mus exist):
        # spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("Postprocessing").getOrCreate()
        spark = SparkSession.builder.appName("Spark Missing Page Info").getOrCreate()

        # Infer the schema, and register the DataFrames as tables.
        page_info_source = spark.sparkContext.textFile(os.path.join(self.data_path, self.page_info))
        page_info = page_info_source.map(self.mapper_page_info)
        page_info_df = spark.createDataFrame(page_info).cache()
        page_info_df.createOrReplaceTempView("info")

        revision_info_source = spark.sparkContext.textFile(os.path.join(self.data_path, self.revision_info))
        revision_info = revision_info_source.map(self.mapper_revisions)
        revision_info_df = spark.createDataFrame(revision_info).cache()
        revision_info_df.createOrReplaceTempView("revision")

        for f in self.page_data:
            page_data_source = spark.sparkContext.textFile(os.path.join(self.data_path, f))  # TODO CHECK if f[0] or f
            page_data = page_data_source.map(self.mapper_page_data)
            page_data_df = spark.createDataFrame(page_data).cache()
            page_data_df.createOrReplaceTempView("data")
            self.data_file_basename = f[:-4]

            missing_source_ids_df = spark.sql("SELECT d.source_id, i.page_title "
                                              "FROM data d LEFT OUTER JOIN info i "
                                              "ON d.source_id = i.page_id WHERE i.page_title IS NULL").distinct()
            number_of_missing_ids = missing_source_ids_df.count()

            if number_of_missing_ids == 0:
                print('Sanity check for potentially missing page information of source_ids passed.')
            else:
                error_filename = self.error_basename+f  # TODO CHECK if f[0] or f
                error_file = os.path.join(self.error_path, error_filename)
                missing_source_ids_df.createOrReplaceTempView("missing")
                missing_source_ids_df = spark.sql('SELECT source_id as id FROM missing').distinct()
                # missing_source_ids = missing_source_ids_df.collect()
                # self.write_list(error_file, missing_source_ids)
                missing_source_ids_df = missing_source_ids_df.coalesce(1)
                missing_source_ids_df.write.format('com.databricks.spark.csv').\
                    option('header', 'false').option('delimiter', '\t').save(error_file)
                print('There is a potential inconsistency! Writing IDs to: ' + error_file)
                print('Number of source_ids missing in the page_info file: ' + str(number_of_missing_ids))
                self.handle_spark_results(self.error_path, error_filename)

                # TODO CHECK IF REGISTER RESULTS WORKS
                if 'errors' in self.project.pinfo['data'].keys():
                    results = self.project.pinfo['data']['errors']
                else:
                    results = {}
                missing_ids_key = self.link_data_type + '_missing_info_ids'
                results[missing_ids_key] = [error_filename]
                self.project.register_results_errors(results)
        return

    def find_unresolvable_target_titles(self):

        self.error_basename = 'missing_target_titles_'
        # Create a SparkSession
        # Note: In case its run on Windows and generates errors use (tmp Folder mus exist):
        # spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp")
        # .appName("Postprocessing").getOrCreate()
        spark = SparkSession.builder.appName("Spark Missing Page Info").getOrCreate()

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

        for f in self.page_data:
            error_data_filename = self.error_basename + 'data_' + f
            error_title_filename = self.error_basename + 'titles_' + f
            error_data_file = os.path.join(self.error_path, error_data_filename)
            error_title_file = os.path.join(self.error_path, error_title_filename)
            # Infer the schema, and register the DataFrames as tables.
            page_data_source = spark.sparkContext.textFile(os.path.join(self.data_path, f))
            page_data = page_data_source.map(self.mapper_page_data)
            page_data_df = spark.createDataFrame(page_data).cache()
            page_data_df.createOrReplaceTempView("data")

            # TODO Check: ob es jetzt richtig funktioniert.
            # Auflösung der Titel funktioniert gerade nur für Cats und nicht für links!
            # GRUND: FEHLER beim Parsing/Processing von
            # Cat data > Hier müsste Category: am Titel dran bleiben. Wenn dies gelöst,
            # dann oben im mapper der page info auch das removal von
            # "Category:" entfernen
            # cat_data_df = spark.sql('SELECT * FROM info WHERE page_ns = 14')
            # cat_data_df.createOrReplaceTempView("info")

            resolved_titles_df = spark.sql("SELECT d.source_id as source, i.page_id as target, d.target_title, d.rev_id as revision "
                                           "FROM data d LEFT OUTER JOIN info i ON UPPER(d.target_title) = UPPER(i.page_title)")
            resolved_titles_df.createOrReplaceTempView("resolved")

            missing_titles_all_df = spark.sql("SELECT source, target_title, revision FROM resolved "
                                              "WHERE target IS NULL")
            missing_titles_all_df.createOrReplaceTempView("missing")

            missing_titles_df = spark.sql("SELECT target_title FROM missing").distinct().coalesce(1)
            missing_titles_df.write.format('com.databricks.spark.csv').option('header', 'false').option('delimiter', '\t').save(error_title_file)
            self.handle_spark_results(self.error_path, error_title_filename)

            missing_titles_all_df = missing_titles_all_df.coalesce(1)
            missing_titles_all_df.write.format('com.databricks.spark.csv').option('header', 'false').option('delimiter', '\t').save(error_data_file)

            self.handle_spark_results(self.error_path, error_data_filename)

            # TODO CHECK IF Register RESULTS WORKS
            if 'errors' in self.project.pinfo['data'].keys():
                results = self.project.pinfo['data']['errors']
            else:
                results = {}

            missing_titles_key = self.link_data_type + '_missing_info_titles_titles'
            results[missing_titles_key] = [error_title_filename]
            missing_data_key = self.link_data_type + '_missing_info_titles_data'
            results[missing_data_key] = [error_data_filename]
            self.project.register_results_errors(results)
        return


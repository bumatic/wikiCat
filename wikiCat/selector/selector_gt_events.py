import os
from datetime import datetime

from wikiCat.selector.selector import Selector


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
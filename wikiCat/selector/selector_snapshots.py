import os
from datetime import datetime

from wikiCat.selector.selector import Selector


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
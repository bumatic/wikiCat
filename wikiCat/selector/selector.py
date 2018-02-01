import findspark
findspark.init()
from pyspark.sql import SparkSession
from wikiCat.processor.spark_processor_graph import SparkProcessorGraph
from dateutil import parser
from datetime import datetime
import pandas as pd
import os

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





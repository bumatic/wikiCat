from wikiCat.processor.spark_processor import SparkProcessor


class SparkProcessorGraph(SparkProcessor):
    def __init__(self, project, fixed='fixed_none', errors='errors_removed'):
        Processor.__init__(self, project, 'graph')
        self.path = self.project.graph_data_path
        self.fixed = fixed
        self.errors = errors
        self.data_status = 'graph__' + self.fixed + '__' + self.errors

from wikiCat.processor.spark_processor import SparkProcessor


class SparkProcessorGraph(SparkProcessor):
    def __init__(self, project): #, fixed='fixed_none', errors='errors_removed'
        SparkProcessor.__init__(self, project, 'graph')

        #TODO CHECK
        self.path = self.project.pinfo['path']['graph']

        #TODO: Remove everywhere else
        #self.fixed = fixed
        #self.errors = errors
        #self.data_status = 'graph__' + self.fixed + '__' + self.errors




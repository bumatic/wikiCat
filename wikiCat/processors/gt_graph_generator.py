from wikiCat.processors.processor import Processor
# from graph_tool.all import *


class GtGraphGenerator(Processor):
    def __init__(self, project, link_data_type, fixed='fixed_none', errors='errors_removed'):
        Processor.__init__(self, project, 'graph')
        self.link_data_type = link_data_type
        self.fixed = fixed
        self.errors = errors
        self.type = self.link_data_type + '_' + self.fixed + '_' + self.errors
        self.nodes = self.data_obj.data[self.type]['nodes']
        self.edges = self.data_obj.data[self.type]['edges']
        self.events = self.data_obj.data[self.type]['events']
        print(self.nodes)
        print(self.edges)
        print(self.events)

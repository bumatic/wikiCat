from wikiCat.processors.GtGraphProcessor import GtGraphProcessor
from graph_tool.all import *
import os
import pprint
import pandas as pd

class Visualizer(GtGraphProcessor):
    def __init__(self, graph):
        GtGraphProcessor.__init__(self, graph)
        self.gt = Graph()
        self.data = self.graph.data
        print(self.edges_location)
        pp = pprint.PrettyPrinter(indent=4)
        v = vars(self)
        pp.pprint(v)

    def snapshots(self, parameter):
        files = self.graph.data[parameter]
        print(files)
        path = self.graph.data
        print(path)
        graph_views = self.create_gt_views(files, path)
        for gv in graph_views:
            self.visualize(gv)

    def load(self, sub_graph=None):
        if sub_graph is not None:
            graph_type = sub_graph
            if 'gt_file' in self.data[graph_type].keys():
                self.gt.load(os.path.join(self.data[graph_type]['location'], self.data[graph_type]['gt_file']))
                return
            else:
                super_graph = self.data[graph_type]['derived_from']
                print(super_graph)
                #self.gt.load(os.path.join(self.data[super_graph]['location'], self.data[super_graph]['gt_file']))
                edges = self.load_edges(os.path.join(self.edges_location, self.edges[0]))
                for e in edges.iterrows():
                    # e[1]['source']
                    # e[1]['target']



        else:
            graph_type = 'main'
            self.gt.load(os.path.join(self.data[graph_type][location], self.data[graph_type]['gt_file']))


    def create_sub_graph(self, sub_graph):

        pass

    def create_gt_views(self, files, path):
        graph_views = []
        for file in files:
            # Create property map of
            os.path.join(path, file)
            # Create graph_view
            # graph_views.append(gv)
            pass
        # return graph_views

    def visualize(self, graph_view):
        pass


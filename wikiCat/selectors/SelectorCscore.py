from wikiCat.selectors.selector import Selector
from wikiCat.processors.PandasProcessor import PandasProcessor
import os

class SelectorCscore(PandasProcessor):
    def __init__(self, graph):
        self.project = self.graph.project
        PandasProcessor.__init__(self, self.project, 'gt_graph')



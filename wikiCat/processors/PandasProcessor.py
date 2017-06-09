from wikiCat.processors.processor import Processor
import pandas as pd


class PandasProcessor(Processor):
    def __init__(self, project, processor_type):
        Processor.__init__(self, project, processor_type)

    def load_events(self, file, cscore=True):
        if cscore:
            events = pd.read_csv(file, header=None, delimiter='\t',
                                 names=['revision', 'source', 'target', 'event', 'cscore'])
        else:
            events = pd.read_csv(file, header=None, delimiter='\t',
                                 names=['revision', 'source', 'target', 'event'])
        return events

    def load_edges(self, file, cscore=True):
        if cscore:
            edges = pd.read_csv(file, header=None, delimiter='\t',
                                names=['source', 'target', 'type', 'cscore'])
        else:
                edges = pd.read_csv(file, header=None, delimiter='\t',
                                    names=['source', 'target', 'type'])
        return edges

    def load_nodes(self, file, cscore=True):
        # Default node columns ['id', 'title', 'ns', ('cscore')]
        if cscore:
            nodes = pd.read_csv(file, header=None, delimiter='\t',
                                names=['id', 'title', 'ns', 'cscore'])
        else:
            nodes = pd.read_csv(file, header=None, delimiter='\t',
                                names=['id', 'title', 'ns', 'cscore'])
        return nodes

    def highest_cscores(self, df, n=100, save=False):
        largest = df.nlargest(n, 'cscore')
        if save:
            print(largest)
            # ToDo Implement saving
            pass
        else:
            print(largest)
        return largest
        pass

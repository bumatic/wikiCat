from wikiCat.processor.processor import Processor
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

    def highest_cscores(self, df, n=100, save=False, outfile=None):
        largest = df.nlargest(n, 'cscore')
        if save:
            if outfile is not None:
                largest.to_csv(outfile, sep='\t', index=False, header=False, mode='w')
            else:
                print('A name for the outfile needs to be passed')
            #print(largest)
            pass
        else:
            print(largest)
        return largest
        pass

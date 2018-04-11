from wikiCat.processor.processor import Processor
import pandas as pd

# TODO CHECK IF na_filter=False can be set everywhere. is drop_na used somewehere????

class PandasProcessor(Processor):
    def __init__(self, project, processor_type):
        Processor.__init__(self, project, processor_type)

    def load_events(self, file, cscore=True):
        if cscore:
            dtype = {
                'revision': float,
                'source': int,
                'target': int,
                'event': str,
                'cscore': float
            }
            events = pd.read_csv(file, header=None, delimiter='\t',
                                 names=['revision', 'source', 'target', 'event', 'cscore'], dtype=dtype, na_filter=False)
        else:
            dtype = {
                'revision': float,
                'source': int,
                'target': int,
                'event': str
            }
            events = pd.read_csv(file, header=None, delimiter='\t',
                                 names=['revision', 'source', 'target', 'event'], dtype=dtype, na_filter=False)
        return events

    def load_edges(self, file, cscore=True):
        if cscore:
            dtype = {
                'source': int,
                'target': int,
                'type': str,
                'cscore': float
            }
            edges = pd.read_csv(file, header=None, delimiter='\t',
                                names=['source', 'target', 'type', 'cscore'], dtype=dtype, na_filter=False)
        else:
            dtype = {
                'source': int,
                'target': int,
                'type': str
            }
            edges = pd.read_csv(file, header=None, delimiter='\t',
                                names=['source', 'target', 'type'], dtype=dtype, na_filter=False)
        return edges

    def load_nodes(self, file, cscore=True):
        # Default node columns ['id', 'title', 'ns', ('cscore')]
        if cscore:
            dtype = {
                'id': int,
                'title': str,
                'ns': int,
                'cscore': float
            }
            nodes = pd.read_csv(file, header=None, delimiter='\t',
                                names=['id', 'title', 'ns', 'cscore'], dtype=dtype, na_filter=False)
        else:
            dtype = {
                'id': int,
                'title': str,
                'ns': int
            }
            nodes = pd.read_csv(file, header=None, delimiter='\t',
                                names=['id', 'title', 'ns'], dtype=dtype, na_filter=False)
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

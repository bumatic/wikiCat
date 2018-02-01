from wikiCat.selector.selector_sub_graph import SubGraph
from wikiCat.selector.selector_snapshots import Snapshots


class SubGraphViews:
    def __init__(self, graph):
        self.graph = graph
        #Snapshots.__init__(self, graph)
        #SubGraph.__init__(self, graph)

    def create(self, subgraph_title, snapshot_title, seed=None, cats=True, subcats=2, supercats=2, links=False,
               inlinks=2, outlinks=2, slice='year', cscore=True, start_date=None, end_date=None):

        print('create subgraph')
        success = SubGraph(self.graph).create(title=subgraph_title, seed=seed, cats=cats, subcats=subcats,
                                              supercats=supercats, links=links, inlinks=inlinks, outlinks=outlinks)

        if not success:
            return
        else:
            print('SUCCESS')

        self.graph.set_working_graph(key=subgraph_title)
        print('create snapshots')
        Snapshots(self.graph).create(snapshot_title, slice=slice, cscore=cscore, start_date=start_date, end_date=end_date)
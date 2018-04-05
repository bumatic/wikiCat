from wikiCat.selector.selector_sub_graph import SubGraph
from wikiCat.selector.selector_snapshots import Snapshots


# ToDo: Where is this used?
class SubGraphViews:
    def __init__(self, graph):
        self.graph = graph

    def create(self, subgraph_title, snapshot_title, seed=None, cats=True, subcats=2, supercats=2, links=False,
               inlinks=2, outlinks=2, slice='year', cscore=True, start_date=None, end_date=None):

        print('Create Subgraph')
        success = SubGraph(self.graph).create(title=subgraph_title, seed=seed, cats=cats, subcats=subcats,
                                              supercats=supercats, links=links, inlinks=inlinks, outlinks=outlinks)

        if not success:
            return
        else:
            print('Subgraph sucessfully created')

        self.graph.set_working_graph(key=subgraph_title)
        print('Create snapshots for subgraph')
        Snapshots(self.graph).create(snapshot_title, slice=slice, cscore=cscore,
                                     start_date=start_date, end_date=end_date)

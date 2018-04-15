from wikiCat.wikiproject import Project
from wikiCat.processor.gt_sub_graph_processor import SubGraphProcessor

mp = Project()
mp.remove_subgraph('contents_2sub')
title = 'snapshots_year'
SubGraphProcessor(mp).internalize_snapshots(title)
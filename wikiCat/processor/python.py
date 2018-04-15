from wikiCat.wikiproject import Project
from wikiCat.processor.gt_sub_graph_processor import SubGraphProcessor

mp = Project()
title = 'snapshots_year'
SubGraphProcessor(mp).internalize_snapshots(title)
from wikiCat.wikiproject import Project
mp = Project()
mp.remove_snapshots('contents_2sub', 'snapshots_year')


mp.remove_subgraph('contents_2sub')
title = 'snapshots_year'
SubGraphProcessor(mp).internalize_snapshots(title)
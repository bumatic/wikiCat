from wikiCat.wikiproject import Project


mp = Project()
mp.get_highest_cscores('nodes', n=10000, cats_only=False)
mp.get_highest_cscores('nodes', n=10000, cats_only=True)
mp.get_highest_cscores('edges', n=10000, cats_only=False)
mp.get_highest_cscores('edges', n=10000, cats_only=True)
mp.get_highest_cscores('events', n=10000, cats_only=False)
mp.get_highest_cscores('events', n=10000, cats_only=True)


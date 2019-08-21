from wikiCat.wikiproject import Project
from wikiCat.processor.graph_data_gererator import GraphDataGenerator
mp = Project()



processing = {
    "graph_data": {
        "page_info": "done",
        "cats": {},
        "links": {
            "enwiki-20180701-pages-meta-history10.xml-p2505803p2535938_links.csv.7z": "init",
            "enwiki-20180701-pages-meta-history13.xml-p5096987p5137296_links.csv.7z": "init"
        }
    }
}

print(processing)

processor = GraphDataGenerator(mp)

for k in processing['links'].keys():
    if processing['links'][k] == 'init':
        processor.generate_manual('links', k, resolve_authors=False)

'''
self.project.pinfo['processing']['graph_data']['links'][link] = \
            self.generate(edge_type, link, resolve_authors=resolve_authors)
'''



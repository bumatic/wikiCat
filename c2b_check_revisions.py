import os
import pandas as pd
import requests
import json
from tqdm import tqdm


event_data_file = os.path.join('manual_graph_data_generation', 'enwiki-20180701-pages-meta-history10.xml-p2505803p2535938_links_events.csv')
event_data = pd.read_csv(event_data_file, delimiter='\t', names=['source', 'target', 'rev_id', 'event'])
print('Number of unique sources')
print(len(event_data.source.drop_duplicates()))

event_data = event_data.drop_duplicates('rev_id')
print(len(event_data))


rev_data_file = os.path.join('manual_graph_data_generation', 'revisions_reduced_new.csv')
rev_data = pd.read_csv(rev_data_file, delimiter='\t', names=['page_id', 'rev_id', 'ts', 'author_id'])
print(len(event_data.merge(rev_data, left_on='rev_id', right_on='rev_id')))
print(len(event_data[event_data.rev_id.isin(rev_data.rev_id)]))

rev_data_file = os.path.join('manual_graph_data_generation', 'revisions_parsed_new.csv')
rev_data = pd.read_csv(rev_data_file, delimiter='\t', names=['page_id', 'rev_id', 'ts', 'author_id'])
print(len(event_data.merge(rev_data, left_on='rev_id', right_on='rev_id')))
print(len(event_data[event_data.rev_id.isin(rev_data.rev_id)]))





#rev_new_2 = rev_data[rev_data.rev_id.isin(rel_rev.rev_id)]








'''
chunksize = 10000000
#rev_data_file = os.path.join('project', '01_data', '01_parsed', 'revisions.csv')
rev_data_file = os.path.join('manual_graph_data_generation', 'revisions_parsed_new.csv')
rev_data = pd.read_csv(rev_data_file, delimiter='\t', names=['page_id', 'rev_id', 'ts', 'author_id'], chunksize=chunksize)

for chunk in rev_data:
    #print (chunk.dtypes)
    print(event_data.merge(chunk, left_on='rev_id', right_on='rev_id'))
    #print(chunk[chunk['page_id'] == 2513427])
'''

'''
def query(request, lang):
    request['action'] = 'query'
    request['format'] = 'json'
    lastContinue = {'continue': ''}
    while True:
        # Clone original request
        req = request.copy()
        # Modify it with the values returned in the 'continue' section of the last result.
        req.update(lastContinue)
        # Call API
        result = requests.get('https://'+lang+'.wikipedia.org/w/api.php', params=req).json()
        if 'error' in result:
            print (result['error'])
        if 'warnings' in result:
            print(result['warnings'])
        if 'query' in result:
            yield result['query']
        if 'continue' not in result:
            return result
        lastContinue = result['continue']

def retrieve_revisions_json (input_array, lang, directory):
    #id_request_array = assemble_api_request_identifiers(input_array, array_dimension)
    id_request_array = input_array
    filename = os.path.join(directory, 'revisions_api_retrieval_test.txt')
    with open(filename, 'w') as outfile:
        outfile.write('{"results": [')
        first = True
        for ids in tqdm(id_request_array, desc='Retrieve revisions'):
            for response in query({'list': 'allrevisions',
                                   'arvlimit': 'max',
                                   'arvprop': 'ids|timestamp|userid|user',
                                   'pageids' : ids}, lang):
                if first:
                    json.dump(response, outfile)
                    first = False
                else:
                    outfile.write(',')
                    json.dump(response, outfile)
        outfile.write(']}')
    return filename


retrieve_revisions_json([2513427], 'en', '')
#https://en.wikipedia.org/w/api.php?action=query&list=allrevisions&pageids=2513427|2517159&arvprop=ids|timestamp|userid|user&arvlimit=max
'''







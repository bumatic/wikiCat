from wikiCat.processors.pandas_processor_graph import PandasProcessorGraph
from dateutil import parser
import math
#import datetime
import pandas as pd
import os


class ControvercyScore(PandasProcessorGraph):
    def __init__(self, project, fixed='fixed_none', errors='errors_removed'):
        PandasProcessorGraph.__init__(self, project)
        self.growth_rate = 1
        self.decay_rate = 0.0000001
        self.start_score = -0.9

    def set_constants(self, growth_rate=1, decay_rate=0.0000001, start_score=-0.9):
        self.growth_rate = growth_rate
        self.decay_rate = decay_rate
        self.start_score = start_score

    def cscore(self, t1, t2, cscore=-0.9):
        delta = (t2-t1)
        cscore = cscore * math.exp(-1 * self.decay_rate * delta) + self.growth_rate
        return cscore

    def calculate_edge_score(self):
        curr = {}
        results = pd.DataFrame(columns=('revision', 'source', 'target', 'event', 'cscore'))
        for file in self.events_files:
            self.load_events(file, columns=['revision', 'source', 'target', 'event'])
            for key, row in self.events.iterrows():
                revision = row['revision']
                source = row['source']
                target = row['target']
                key = str(source)+'|'+str(target)
                event = row['event']
                if key in curr.keys():
                    past_cscore = curr[key][0]
                    past_revision = curr[key][1]
                    cscore = self.cscore(past_revision, revision, cscore=past_cscore)
                else:
                    cscore = -0.9
                print(cscore)
                curr[key] = [cscore, revision]
                curr_results = pd.DataFrame([[revision, source, target, event, cscore]], columns=('revision', 'source', 'target', 'event', 'cscore'))
                results = results.append(curr_results, ignore_index=True)
            results.to_csv(file+'_test', sep='\t', index=False, header=False, mode='w')

    def calculate_node_score(self):
        curr={}
        results = pd.DataFrame(columns=('id', 'title', 'ns', 'cscore'))
        for file in self.events_files:
            self.load_events(file, columns=['revision', 'source', 'target', 'event', 'cscore'])
            for key, row in self.events.iterrows():
                if row[1] in curr.keys():
                    curr[row[1]] = [curr[row[1]][0]+float(row[4]), curr[row[1]][0]+1]
                else:
                    curr[row[1]] = [float(row[4]), 1]
        for file in self.nodes_files:
            self.load_nodes(file, columns=['id', 'title', 'ns'])
            for key, row in self.nodes.iterrows():
                id = row[0]
                title = row[1]
                ns = row[2]
                cscore = curr[id][0]/curr[id][1]
                curr_results = pd.DataFrame([[id, title, ns, cscore]], columns=('id', 'title', 'ns', 'cscore'))
                results = results.append(curr_results, ignore_index=True)
            results.to_csv(file + '_test', sep='\t', index=False, header=False, mode='w')

    '''
    def cscore_test(self):
        t1 = '2003-04-25 22:18:38'
        t1 = parser.parse(t1)
        print(t1)
        t2 = '2003-12-26 16:55:41'
        t2 = parser.parse(t2)
        print(t2)
        print(type(t2))
        delta = t2-t1
        print(delta)
        print(delta.total_seconds())
        print(type(delta.total_seconds()))
        #cscore = 1
        try:
            cscore
        except NameError:
            cscore = self.start_score
        print('start score:' + str(cscore))
        print('decay factor: '+ str(math.exp(-1 * self.decay_rate * delta.total_seconds())))
        cscore = cscore * math.exp(-1 * self.decay_rate * delta.total_seconds()) + self.growth_rate
        print(cscore)
    '''

    def calc_cscore_test(self):
        df = pd.DataFrame([['a', 'b'], ['c', 'd'], ['e', 'F']], columns=list('AB'))
        print(df)
        for key, row in df.iterrows():
            print(row['B'])
            print()


import findspark
findspark.init()
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, avg
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

    def set_constants(self, growth_rate=1, decay_rate=0.0000001, start_score=0):
        self.growth_rate = growth_rate
        self.decay_rate = decay_rate
        self.start_score = start_score

    def cscore(self, t1, t2, cscore=-0.9):
        delta = (t2-t1)
        cscore = cscore * math.exp(-1 * self.decay_rate * delta) + self.growth_rate
        return cscore

    def progress(self, counter):
        chunks = 10**5
        if counter >= chunks:
            print(str(chunks)+' edges have been processed')
            return 0
        else:
            counter = counter + 1
            return counter

    def mapper_nodes(self, line):
        fields = line.split('\t')
        id = fields[0]
        title = fields[1]
        ns = fields[3]
        return Row(id=id, title=title, ns=ns)

    def mapper_events(self, line):
        fields = line.split('\t')
        if len(fields) == 4:
            revision = float(fields[0])
            source = fields[1]
            target = fields [2]
            event = fields[3]
            return Row(revision=revision, source=source, target=target, event=event)
        elif len(fields) == 5:
            revision = float(fields[0])
            source = fields[1]
            target = fields[2]
            event = fields[3]
            cscore = fields[4]
            return Row(revision=revision, source=source, target=target, event=event, cscore=cscore)
        else:
            print('Error while mapping events')
            return

    def calculate_edge_score_spark(self):
        print(self.path)
        print(self.events_files)

        # Create a SparkSession
        # Note: In case its run on Windows and generates errors use (tmp Folder mus exist):
        # spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("Postprocessing").getOrCreate()
        spark = SparkSession.builder.appName("Calculate_Controvercy_Score_Edges").getOrCreate()

        for file in self.events_files:
            results = pd.DataFrame(columns=('revision', 'source', 'target', 'event', 'cscore'))
            tmp_results_file = 'tmp_' + file
            events_source = spark.sparkContext.textFile(os.path.join(self.path, file))
            events = events_source.map(self.mapper_events)
            events_df = spark.createDataFrame(events).cache()
            events_df.createOrReplaceTempView("events")
            events_grouped_df = events_df.groupBy('source', 'target').agg(collect_list('revision').alias('revision'))
            events_grouped_df.createOrReplaceTempView("events_grouped")

            #events_grouped_df = spark.sql('SELECT e.source, e.target, e.event, g.revision FROM '
            #                              'events e JOIN events_grouped g ON e.source = g.source '
            #                              'AND e.target = g.target')

            #TODO Auflösung von Resultaten
            edge_events = events_grouped_df.rdd.map(self.process_spark_list).collect()

            with open(os.path.join(self.path, tmp_results_file), 'w', newline='') as outfile:
                for edge in edge_events:
                    for event in edge:
                        outfile.write(str(event[0]) + '\t' + str(event[1]) + '\t' + str(event[2]) + '\t' +str(event[3]) + '\t' + str(event[4]) + '\n')

                #curr_results = pd.DataFrame(edge_event, columns=('revision', 'source', 'target', 'event', 'cscore'))
                #results = results.append(curr_results, ignore_index=True)
                #for event in edge:
                #    print(event[0])
                #    print(event[1])
            #results.to_csv(os.path.join(self.path, tmp_results_file), sep='\t', index=False, header=False, mode='w')

    def process_spark_list(self, row):
        source = row[0]
        target = row[1]
        event = row[2]
        ts_list = row[3]
        results = []
        for i in range(len(ts_list)):
            if i == 0:
                cscore = 0
            else:
                delta = ts_list[i] - ts_list[i-1]
                cscore = cscore * math.exp(-1 * self.decay_rate * delta) + self.growth_rate
            results.append([ts_list[i], source, target, event, cscore])
        return results

    def calculate_node_score_spark(self):
        print(self.path)
        print(self.events_files)
        print(self.nodes_files)

        # Create a SparkSession
        # Note: In case its run on Windows and generates errors use (tmp Folder mus exist):
        # spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("Postprocessing").getOrCreate()
        spark = SparkSession.builder.appName("Calculate_Controvercy_Score_Edges").getOrCreate()

        nodes_source = spark.sparkContext.textFile(os.path.join(self.path, self.nodes_files))
        nodes = nodes_source.map(self.mapper_nodes)
        nodes_df = spark.createDataFrame(nodes).cache()
        nodes_df.createOrReplaceTempView("nodes")

        for file in self.events_files:
            tmp_results_file = 'tmp_' + file

            events_source = spark.sparkContext.textFile(os.path.join(self.path, file))
            events = events_source.map(self.mapper_events)
            events_df = spark.createDataFrame(events).cache()
            events_df.createOrReplaceTempView("events")

            source_df = spark.sql('SELECT source as node, cscore FROM events')
            target_df = spark.sql('SELECT target as node, cscore FROM events')
            node_cscores_df = source_df.union(target_df)
            avg_node_cscores_df = node_cscores_df.groupby('node').agg(avg('cscore').alias('avg_cscore'))
            avg_node_cscores_df.show()






        '''
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


    def calculate_edge_score2(self):
        counter = 0
        for file in self.events_files:
            results = pd.DataFrame(columns=('revision', 'source', 'target', 'event', 'cscore'))
            self.load_events(file, columns=['revision', 'source', 'target', 'event'])
            tmp_results_file = 'tmp_' + file
            for group, events in self.events.groupby(['source', 'target']):
                events = events.sort_values(['revision'])
                source = group[0]
                target = group[1]
                # key = str(source) + '|' + str(target)
                past_revision = None
                past_cscore = None
                for event in events.iterrows():
                    revision = event[1]['revision']
                    event_type = event[1]['event']
                    if past_cscore is None:
                        cscore = 0
                        past_revision = revision
                    else:
                        cscore = self.cscore(past_revision, revision, cscore=past_cscore)

                    curr_results = pd.DataFrame([[revision, source, target, event_type, cscore]], columns=('revision', 'source', 'target', 'event', 'cscore'))
                    results = results.append(curr_results, ignore_index=True)
                    past_cscore = cscore

                # TODO ONCE TEST WORKED: WRITING RESULTS TO FILE AND REPLACING ORIGINAL FILE WITH RESULTS
                counter = self.progress(counter)
            results.to_csv(os.path.join(self.path, tmp_results_file), sep='\t', index=False, header=False, mode='w')
            os.remove(os.path.join(self.path, file))
            os.rename(os.path.join(self.path, tmp_results_file), os.path.join(self.path, file))


    def calculate_edge_score(self):
        curr = {}
        for file in self.events_files:
            chunksize = 10 ** 6
            for chunk in pd.read_csv(os.path.join(self.path, file), delimiter='\t',
                                     names=['revision', 'source', 'target', 'event', 'cscore'],
                                     chunksize=chunksize):
                results = pd.DataFrame(columns=('revision', 'source', 'target', 'event', 'cscore'))
                for key, row in chunk.iterrows():
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
                        cscore = self.start_score
                    #print(cscore)
                    curr[key] = [cscore, revision]
                    curr_results = pd.DataFrame([[revision, source, target, event, cscore]], columns=('revision', 'source', 'target', 'event', 'cscore'))
                    results = results.append(curr_results, ignore_index=True)
                #TODO ONCE TEST WORKED: WRITING RESULTS TO FILE AND REPLACING ORIGINAL FILE WITH RESULTS
                results.to_csv(file+'_test', sep='\t', index=False, header=False, mode='a')
                print('A chunk is done')

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


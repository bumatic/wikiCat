# from wikiCat.wikiproject import Project
from wikiCat.processors.pandas_processor_graph import PandasProcessorGraph
from dateutil import parser


class Selector(PandasProcessorGraph):
    def __init__(self, project, fixed='fixed_none', errors='errors_removed', start_date=None):
        PandasProcessorGraph.__init__(self, project, fixed=fixed, errors=errors)
        self.project = project
        self.end_date = self.project.dump_date.timestamp()

        if start_date is None:
            # TODO Assign start date from project. For this the inclusion in the project must work.
            # once it works assign: self.project.start_date
            self.start_date = parser.parse('2003-01-01').timestamp()

    def temporal_views(self, slice='year', cscore=True, start_date=None):
        assert slice is 'year' or 'month' or 'day', 'Error. Pass a valid value for slice: year, month, day.'
        assert type(cscore) is bool, 'Error. A bool value is expected for cscore signalling, if data file contains ' \
                                     'cscore.'
        if start_date is not None:
            self.start_date = parser.parse(start_date).timestamp()
        if slice == 'day':
            delta = 86400
        elif slice == 'month':
            delta = 2592000
        elif slice == 'year':
            delta = 31536000
        results = {}
        tmp_results = {}
        last_slice = self.start_date.timestamp()
        for file in self.events_files:
            if cscore:
                self.load_events(file, columns=['revision', 'source', 'target', 'event', 'cscore'])
            else:
                self.load_events(file, columns=['revision', 'source', 'target', 'event'])
            for revision, events in self.events.groupby('revision'):
                if (revision - last_slice) > delta and revision >= self.start_date:
                    results[last_slice] = tmp_results
                    last_slice = revision
                for event in events.iterrows():
                    if event[1]['event'] == 'start':
                        tmp_results[str(event[1]['source'])+'|'+str(event[1]['target'])] = True
                    elif event[1]['event'] == 'end':
                        tmp_results[str(event[1]['source']) + '|' + str(event[1]['target'])] = False

        results[self.end_date] = tmp_results
        # TODO: Implement handling of results

    def temporal_views_spark(self, slice='year', cscore=True, start_date=None):
        assert slice is 'year' or 'month' or 'day', 'Error. Pass a valid value for slice: year, month, day.'
        assert type(cscore) is bool, 'Error. A bool value is expected for cscore signalling, if data file contains ' \
                                     'cscore.'

        # TODO Implementieren
        '''
        IDEE:
            1. Generate list of desired dates/cuts
            2. For each slice:
                A) SELECT max(revision) as revision, source, target, type FROM events WHERE revision < slice_date'
                B) SELECT source, target FROM events where type = 'start'
                B) Collect results and add list to results-dict, with slice date as key
        '''



    def sub_graph(self, seed=None, depth=3):
        pass

    def sub_graph_views(self):
        # combination of temporal_views and sub_graph_views
        pass




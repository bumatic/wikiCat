from wikiCat.data.data import Data
#import os
#import bz2


class WikiData(Data):
    def __init__(self, project, data_type):
        Data.__init__(self, project, data_type)
        self.data = self.project.data_desc[data_type]

    def add_dump_data(self):
        print('Add dump data is not yet implemented')
        # 2DO

    def add_parsed_data(self, page_info=None, revision_info=None, cat_data=None, link_data=None):
        if page_info is not None:
            if self.check_files(page_info):
                self.data['page_info'] = self.check_filetype(page_info)
            else:
                print('page_info file(s) do not exist')
        if revision_info is not None:
            if self.check_files(revision_info):
                self.data['revision_info'] = self.check_filetype(revision_info)
            else:
                print('revision_info file(s) do not exist')
        if cat_data is not None:
            if self.check_files(cat_data):
                self.data['cat_data'] = self.check_filetype(cat_data)
            else:
                print('cat_data file(s) do not exist')
        if link_data is not None:
            if self.check_files(link_data):
                self.data['link_data'] = self.check_filetype(link_data)
            else:
                print('link_data file(s) do not exist')
        return self.data

    def add_graph_data(self, nodes=None, edges=None, events=None, gt=None, fixed='fixed_none', errors='errors_removed',
                       override=False):
        self.data_status = 'graph__'+fixed+'__'+errors
        self.data_path = self.project.graph_data_path

        #TODO CHECK FOR ERRORS in ADDING GRAPH DATA

        if self.data_status in self.data.keys() and not override:
            print('This type of graph data has already been added. Pass override=True to replace it.')
            return
        else:
            self.data[self.data_status] = {}
            if nodes is not None:
                if self.check_files(nodes):
                    self.data[self.data_status]['nodes'] = nodes
                else:
                    print('nodes file(s) do not exist')
            if edges is not None:
                if self.check_files(edges):
                    self.data[self.data_status]['edges'] = edges
                else:
                    print('edges file(s) do not exist')
            if events is not None:
                if self.check_files(events):
                    self.data[self.data_status]['events'] = events
                else:
                    print('events file(s) do not exist')
            # TODO Potentially to take out
            if gt is not None:
                if self.check_files(gt):
                    self.data[self.data_status]['gt'] = gt
                else:
                    print('gt graph file(s) do not exist')
        return self.data

    def add_error_data(self, error_data=[], error_type='error', override=False):
        if error_type in self.data.keys() and not override:
            print('This type of graph data has already been added. Pass override=True to replace it.')
        else:
            self.data[error_type] = error_data
        return self.data

    def init_data(self, data): #ARGS?
        self.data = data
        #self.project.update_data_desc(self.data_type, data)
        # TODO: Low priority: IMPLEMENT CHECK IF EVERYTHING EXISTS?!?!

    def get_data_desc(self):
        return self.data

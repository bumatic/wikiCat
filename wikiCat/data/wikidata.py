import os
import bz2

class WikiData:
    def __init__(self, project, data_type):
        self.project = project
        self.path = self.project.path
        self.data_type = data_type
        self.data_status = ''
        self.data = {}

        if self.data_type == 'dump':
            self.data_path = self.project.dump_data_path
        elif self.data_type == 'parsed':
            self.data_path = self.project.parsed_data_path
        elif self.data_type == 'graph':
            self.data_path = self.project.graph_data_path
        elif self.data_type == 'error':
            self.data_path = self.project.error_data_path
        else:
            print('Please enter a valid data_type: dump, parsed, graph, error')

    def add_dump_data(self):
        print('Add dump data is not yet implemented')
        # 2DO

    def add_parsed_data(self, page_info=[], revision_info=[], cat_data=[], link_data=[]): #ARGS?
        if self.check_files(page_info):
            self.data['page_info'] = self.check_filetype(page_info)
        else:
            print('page_info file(s) do not exist')
        if self.check_files(revision_info):
            self.data['revision_info'] = self.check_filetype(revision_info)
        else:
            print('revision_info file(s) do not exist')
        if self.check_files(cat_data):
            self.data['cat_data'] = self.check_filetype(cat_data)
        else:
            print('cat_data file(s) do not exist')
        if self.check_files(link_data):
            self.data['link_data'] = self.check_filetype(link_data)
        else:
            print('link_data file(s) do not exist')
        print (self.data)
        return self.data

    def add_graph_data(self, nodes=[], edges=[], events=[], fixed='fixed_none', errors='errors_removed',
                       override=False):
        self.data_status = 'graph__'+fixed+'__'+errors

        # CHECK FOR ERRORS in ADDING GRAPH DATA

        if self.data_status in self.data.keys() and not override:
            print('This type of graph data has already been added. Pass override=True to replace it.')
            return
        else:
            self.data[self.data_status] = {}

        if self.check_files(nodes):
            self.data[self.data_status]['nodes'] = nodes
        else:
            print('nodes file(s) do not exist')
        if self.check_files(edges):
            self.data[self.data_status]['edges'] = edges
        else:
            print('edges file(s) do not exist')
        if self.check_files(events):
            self.data[self.data_status]['events'] = events
        else:
            print('events file(s) do not exist')
        print(self.data)
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
        # 2DO
        # IMPLEMENT CHECK IF EVERTHING EXISTS?


    def check_files(self, file_list):
        exists = True
        for f in file_list:
            print(os.path.join(self.data_path, f))
            if not os.path.exists(os.path.join(self.data_path, f)):
                exists = False
        return exists

    def check_filetype(self, file_list):
        new_list = []
        for f in file_list:
            if f[-3:] == 'bz2':
                new_list.append([f, 'bz2'])
            elif f[-3:] == 'csv':
                new_list.append([f, 'csv'])
            else:
                new_list.append([f, 'unknown'])
        return new_list

    def file_handling(self, source_file, action):
        if action == 'decompress':
            file = source_file[:-4]
            with open(file, 'wb') as new_file, open(source_file, 'rb') as source:
                decompressor = bz2.BZ2Decompressor()
                for data in iter(lambda: source.read(1024000), b''):
                    new_file.write(decompressor.decompress(data))
            return file
        elif action == 'compress':
            file = source_file + '.bz2'
            with open(source_file, 'rb') as input:
                with bz2.BZ2File(file, 'wb', compresslevel=9) as output:
                    shutil.copyfileobj(input, output)
            return file
        elif action == 'remove':
            os.remove(source_file)
            return

    def decompress_data_files(self, remove_compressed=False):
        new_data_dict = {}
        for type, file_list in self.data.items():
            new_file_list = []
            for f in file_list:
                if f[1] == 'bz2':
                    new_file = self.file_handling(os.path.join(self.data_path, f[0]), 'decompress')
                    print(new_file)
                    new_file_list.append([f[0][:-4], 'csv'])
                elif f[1] == 'csv':
                    new_file_list.append([f[0], 'csv'])
            new_data_dict[type] = new_file_list
        if remove_compressed:
            for type, file_list in self.data.items():
                for f in file_list:
                    if f[1] == 'bz2':
                        self.file_handling(os.path.join(self.data_path, f[0]), 'remove')
        self.data = new_data_dict
        return

    def get_data_desc(self):
        return self.data

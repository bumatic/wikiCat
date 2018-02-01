import os
import bz2
import shutil


class Data:
    def __init__(self, project, data_type):
        assert data_type in ['dump', 'parsed', 'graph', 'gt_graph', 'error'], \
            'ERROR. Please pass a valid data_type: dump, parsed, graph, gt_graph, error'
        self.project = project
        self.path = self.project.path
        self.data_type = data_type
        if self.data_type == 'dump':
            self.data_path = self.project.dump_data_path
        elif self.data_type == 'parsed':
            self.data_path = self.project.parsed_data_path
        elif self.data_type == 'graph':
            self.data_path = self.project.graph_data_path
        elif self.data_type == 'error':
            self.data_path = self.project.error_data_path
        elif self.data_type == 'gt_graph':
            self.data_path = self.project.gt_graph_path
        self.data_status = None  # ''
        self.data = {}

    def check_files(self, file_list):
        exists = True
        for f in file_list:
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

import os
import bz2
import shutil
import subprocess


class Data:
    def __init__(self, project, data_type):
        assert data_type in ['parsed', 'graph', 'gt_graph'], \
            'ERROR. Please pass a valid data_type: parsed, graph, gt_graph'
        self.project = project
        self.data_type = data_type
        self.data_path = self.project.pinfo['path'][data_type]
        if data_type == 'parsed' or data_type == 'graph':
            self.data = self.project.pinfo['data'][data_type]
        elif data_type == 'gt_graph':
            self.data = self.project.pinfo[data_type]


    def check_file(self, file):
        if os.path.exists(os.path.join(self.data_path, file)):
            exists = True
        else:
            exists = False
        return exists

    def check_files(self, file_list):
        exists = True
        for f in file_list:
            if not os.path.exists(os.path.join(self.data_path, f)):
                exists = False
        return exists

    @staticmethod
    def check_file_type(file_list):
        new_list = []
        for f in file_list:
            if f[-3:] == 'bz2':
                new_list.append([f, 'bz2'])
            elif f[-2:] == '7z':
                new_list.append([f, '7z'])
            elif f[-3:] == 'csv':
                new_list.append([f, 'csv'])
            else:
                new_list.append([f, 'unknown'])
        return new_list

    @staticmethod
    def file_handling(source_file, action):
        if action == 'decompress':
            if source_file[-3:] == 'bz2':
                decompressed_file = source_file[:-4]
                with open(decompressed_file, 'wb') as new_file, open(source_file, 'rb') as source:
                    decompressor = bz2.BZ2Decompressor()
                    for data in iter(lambda: source.read(1024000), b''):
                        new_file.write(decompressor.decompress(data))
                return decompressed_file
            elif source_file[-2:] == '7z':
                subprocess.call(['7z', 'e', source_file])
                return source_file[:-3]
            else:
                return source_file
        elif action == 'compress':
            compressed_file = source_file + '.7z'
            subprocess.call(['7z', 'a', compressed_file, source_file])
            # Todo: Check if the file can be removed safely.
            # os.remove(source_file)
            return compressed_file
        elif action == 'remove':
            os.remove(source_file)
            return

    def process_file_list(self, file_list, action, remove):
        file_list = self.check_file_type(file_list)
        new_file_list = []
        for f in file_list:
            new_file = self.file_handling(os.path.join(self.data_path, f[0]), action)
            new_file_list.append([new_file])
        if remove:
            if action == 'decompress':
                for f in file_list:
                    if f[1] == 'bz2' or f[1] == '7z':
                        self.file_handling(os.path.join(self.data_path, f[0]), 'remove')
            elif action == 'compress':
                if f[1] == 'csv':
                    self.file_handling(os.path.join(self.data_path, f[0]), 'remove')
        return new_file_list

    def process_data_files(self, action, remove=False):
        new_data_dict = {}
        for key, value in self.data.items():
            if type(value) == list:
                new_data_dict[key] = self.process_file_list(value, action, remove)
            if type(value) == str:
                new_data_dict[key] = self.file_handling(os.path.join(self.data_path, value), action)
                if remove:
                    os.remove(os.path.join(self.data_path, value))
        self.data = new_data_dict
        return self.data


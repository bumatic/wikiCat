import os
import json
from wikiCat.data.wikidata import WikiData
from wikiCat.processors.oldest_revision import OldestRevision
from dateutil import parser

class Project:
    def __init__(self, path):

        self.path = path
        self.project_title = ''
        self.project_description = ''
        self.project_status = '000'
        self.pinfo = {}
        self.pinfo_file = os.path.join(self.path, '_project_info.json')
        self.log_path = os.path.join(self.path, '01_logs')
        self.data_path = os.path.join(self.path, '02_data')
        self.test_data_path = os.path.join(self.data_path, '00_testdata')
        self.dump_data_path = os.path.join(self.data_path, '01_dump')
        self.parsed_data_path = os.path.join(self.data_path, '02_parsed')
        self.graph_data_path = os.path.join(self.data_path, '03_graph')
        self.error_data_path = os.path.join(self.data_path, '04_error')
        self.results_path = os.path.join(self.path, '03_results')
        self.data_objs = {}
        self.data_desc = {}
        self.start_date = None
        self.dump_date = None

    def create_project(self, title='New WikiCat Project', description='This is a WikiCat Project', dump_date=None):
        if not os.path.exists(os.path.join(os.getcwd(), self.pinfo_file)):
            if not os.path.isdir(os.path.join(os.getcwd(), self.log_path)):
                os.makedirs(os.path.join(os.getcwd(), self.log_path))
            if not os.path.isdir(os.path.join(os.getcwd(), self.data_path)):
                os.makedirs(os.path.join(os.getcwd(), self.data_path))
            if not os.path.isdir(os.path.join(os.getcwd(), self.test_data_path)):
                os.makedirs(os.path.join(os.getcwd(), self.test_data_path))
            if not os.path.isdir(os.path.join(os.getcwd(), self.dump_data_path)):
                os.makedirs(os.path.join(os.getcwd(), self.dump_data_path))
            if not os.path.isdir(os.path.join(os.getcwd(), self.parsed_data_path)):
                os.makedirs(os.path.join(os.getcwd(), self.parsed_data_path))
            if not os.path.isdir(os.path.join(os.getcwd(), self.graph_data_path)):
                os.makedirs(os.path.join(os.getcwd(), self.graph_data_path))
            if not os.path.isdir(os.path.join(os.getcwd(), self.error_data_path)):
                os.makedirs(os.path.join(os.getcwd(), self.error_data_path))
            if not os.path.isdir(os.path.join(os.getcwd(), self.results_path)):
                os.makedirs(os.path.join(os.getcwd(), self.results_path))

            self.project_title = title
            self.project_description = description
            self.project_status = '001'
            if dump_date is not None:
                self.dump_date = parser.parse(dump_date).timestamp()
            self.save_project()

            print('A new Project has been created.')
        else:
            print('The file project_info.json already exists. Try loading the project.')
        return

    def load_project(self):
        if os.path.exists(os.path.join(os.getcwd(), self.pinfo_file)):
            with open(os.path.join(os.getcwd(), self.pinfo_file), 'r') as info_file:
                self.pinfo = json.load(info_file)
            info_file.close()
            if 'title' in self.pinfo.keys():
                self.project_title = self.pinfo['title']
            else:
                pass
            if 'description' in self.pinfo.keys():
                self.project_description = self.pinfo['description']
            else:
                pass
            if 'status' in self.pinfo.keys():
                self.project_status = self.pinfo['status']
            else:
                pass
            if 'path' in self.pinfo.keys():
                self.path = self.pinfo['path']
            else:
                pass
            if 'start_date' in self.pinfo.keys():
                self.start_date = parser.parse(self.pinfo['start_date'])
                pass
            else:
                pass
            if 'dump_date' in self.pinfo.keys():
                self.dump_date = parser.parse(self.pinfo['dump_date'])
                pass
            else:
                pass
            if 'dump' in self.pinfo['data'].keys():
                print('LOADING DUMP DATA FROM PROJECT FILE NEEDS TO BE IMPLEMENTED')
            else:
                pass
            if 'parsed' in self.pinfo['data'].keys():
                self.data_objs['parsed'] = WikiData(self, 'parsed')
                self.data_objs['parsed'].init_data(self.pinfo['data']['parsed'])
                self.data_desc['parsed'] = self.data_objs['parsed'].get_data_desc()
            else:
                pass
            if 'graph' in self.pinfo['data'].keys():
                self.data_objs['graph'] = WikiData(self, 'error')
                self.data_objs['graph'].init_data(self.pinfo['data']['graph'])
                self.data_desc['graph'] = self.data_objs['graph'].get_data_desc()
            else:
                pass
            if 'error' in self.pinfo['data'].keys():
                self.data_objs['error'] = WikiData(self, 'error')
                self.data_objs['error'].init_data(self.pinfo['data']['error'])
                self.data_desc['error'] = self.data_objs['error'].get_data_desc()
            else:
                pass


            '''
            data_types = ['raw', 'parsed', 'graph', 'error']

            for dtype in data_types:
                if dtype in self.pinfo['data'].keys():
                    self.data_objs[dtype] = WikiData(self, dtype)
                    self.data_objs[dtype].init_data(self.pinfo['data'][dtype])
                    self.data_desc[dtype] = self.data_objs[dtype].get_data_desc()
            '''

        else:
            print('Project cannot be loaded: The file _project_info.json does not exist. Try creating a new project.')
        return

    def save_project(self):
        self.pinfo = {}
        self.pinfo['title'] = self.project_title
        self.pinfo['description'] = self.project_description
        self.pinfo['status'] = self.project_status
        self.pinfo['path'] = self.path
        self.pinfo['data'] = self.data_desc
        if self.start_date is not None:
            self.pinfo['start_date'] = str(self.start_date)
        if self.dump_date is not None:
            self.pinfo['dump_date'] = str(self.dump_date)

        with open(self.pinfo_file, 'w') as info_file:
            json.dump(self.pinfo, info_file, sort_keys=True, indent=4)
        return

    def add_data(self, dtype, page_info=None, revision_info=None, cat_data=None, link_data=None, error_data=None,
                 error_type='error', nodes=None, edges=None, events=None, gt=None, fixed='fixed_none',
                 errors='errors_removed', override=False):
        if dtype == 'dump':
            print('ADD DUMP - NOT YET IMPLEMENTED')
        elif dtype == 'parsed':
            if 'parsed' in self.data_desc.keys() and not override:
                print(
                    'Data of type »' + dtype + '« has already been initialized. '
                                               'You can override it by passing True for the »override« kwarg.')
            else:
                self.data_objs[dtype] = WikiData(self, dtype)
                self.data_desc[dtype] = self.data_objs[dtype].add_parsed_data(page_info=page_info,
                                                                              revision_info=revision_info,
                                                                              cat_data=cat_data, link_data=link_data,
                                                                              override=override)
                self.save_project()
        elif dtype == 'graph':
            try:
                self.data_desc[dtype] = self.data_objs[dtype].add_graph_data(nodes=nodes, edges=edges,
                                                                             events=events, gt=gt, fixed=fixed,
                                                                             errors=errors, override=override)

            except:
                self.data_objs[dtype] = WikiData(self, dtype)
                self.data_desc[dtype] = self.data_objs[dtype].add_graph_data(nodes=nodes, edges=edges,
                                                                             events=events, gt=gt, fixed=fixed,
                                                                             errors=errors, override=override)

            self.save_project()
        elif dtype == 'error':
            try:
                self.data_desc[dtype] = self.data_objs[dtype].add_error_data(error_data=error_data, error_type=error_type)
            except:
                self.data_objs[dtype] = WikiData(self, dtype)
                self.data_desc[dtype] = self.data_objs[dtype].add_error_data(error_data, error_type)
            self.save_project()
        else:
            print('Please enter a valid type: dump, parsed, cleaned, graph, error')
        return

    def get_title(self):
        return self.project_title

    def get_description(self):
        return self.project_description

    def get_status(self):
        return self.project_status

    def get_start_date(self):
        if self.start_date is not None:
            return self.start_date
        else:
            return 'No start date has been calculated yet. Use the method find_start_date(self).'

    def get_dump_date(self):
        if self.dump_date is not None:
            return self.dump_date
        else:
            return 'No dump date has been set yet. Use the method set_dump_date to set it..'

    def set_title(self, title):
        self.project_title = title
        self.save_project()

    def set_description(self, description):
        self.project_description = description
        self.save_project()

    def set_status(self, status):
        self.project_status = status
        self.save_project()

    def set_dump_date(self, date):
        self.dump_date = parser.parse(date)
        self.save_project()

    def find_start_date(self):
        if 'graph' in self.data_desc.keys():
            # TODO: This needs some testing. Probably an array is returned and the vorrect value needs to be accessed.
            self.start_date = OldestRevision(self).get()
            self.save_project()
        else:
            print('The calculation of the start date requires graph data')

    def update_data_desc(self, data_type, info):
        self.data_desc[data_type] = info
        self.save_project()
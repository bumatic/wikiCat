import os
import shutil
import json
from wikiCat.data.wikigraph import WikiGraph
from dateutil import parser
from wikiCat.processor.graph_data_gererator import GraphDataGenerator
from wikiCat.processor.parsed_data_error_check import ParsedDataErrorCheck
from wikiCat.processor.controvercy_score import ControvercyScore
from wikiCat.visualizer.arf_visualizer import ARF
from wikiCat.visualizer.sfdp_visualizer import SFDP
from wikiCat.visualizer.rtl_visualizer import RTL
from wikiCat.visualizer.fr_visualizer import FR

#from wikiCat.data.reworked_wikidata import WikiData
#from wikiCat.processor.reworked_gt_graph_generator import GtGraphGenerator
#from wikiCat.processor.reworked_gt_sub_graph_processor import SubGraphProcessor
#from wikiCat.selector.reworked_selector_sub_graph import SubGraph
#from wikiCat.selector.reworked_selector_snapshots import Snapshots
from wikiCat.processor.oldest_revision import OldestRevision


class Project:
    def __init__(self, project_path='project', project_info='project_info.json', title='New WikiCat Project',
                 description='This is a WikiCat Project', dump_date=None, init=False):

        gt_props_path = os.path.join(project_path, '00_visualization_properties')
        data_path = os.path.join(project_path, '01_data')
        gt_graphs_path = os.path.join(project_path, '02_gt_graphs')
        results_path = os.path.join(project_path, '03_results')
        data_parsed_path = os.path.join(data_path, '01_parsed')
        data_graph_path = os.path.join(data_path, '02_data_graph')
        data_errors_path = os.path.join(data_path, '03_errors')

        self.pinfo = {'project_base': project_path,
                      'project_info': project_info,
                      'path': {'data': data_path,
                               'parsed': data_parsed_path,
                               'graph': data_graph_path,
                               'errors': data_errors_path,
                               'gt_graph': gt_graphs_path,
                               'gt_props': gt_props_path,
                               'results': results_path
                               },
                      'data': {},
                      'gt_graph': {}
                      }

        self.data = {}
        self.graph = None

        if os.path.isdir(self.pinfo['project_base']):
            success = self.load_project()
            if success:
                pass
            else:
                if init:
                    self.create_project(title, description, dump_date)
                else:
                    print('Project path exists, but no project file can be found. Either pass the correct file name '
                          'to load an existing project or set init to True to initialize a new project.')
        elif not os.path.isdir(self.pinfo['project_base']) and not init:
            print('Project path does not yet exist. Check project path to load a preexisting project'
                  'or set init to True to initialize a new project.')
        elif not os.path.isdir(self.pinfo['project_base']) and init:
            self.create_project(title, description, dump_date)

    def create_project(self, title, description, dump_date):
        if not os.path.isdir(self.pinfo['project_base']):
            os.makedirs(self.pinfo['project_base'])
        if not os.path.exists(os.path.join(os.getcwd(), self.pinfo['project_base'], self.pinfo['project_info'])):
            for key, value in self.pinfo['path'].items():
                if not os.path.isdir(value):
                    os.makedirs(value)
            self.pinfo['project_title'] = title
            self.pinfo['project_description'] = description
            if dump_date is not None:
                self.pinfo['dump_date'] = parser.parse(dump_date).timestamp()
            self.save_project()
            print('A new Project has been created.')
        else:
            print('Ups. Something went wrong. A project file already exists in this location. '
                  'Try loading the project.')
        return

    def load_project(self):
        pinfo_file = os.path.join(self.pinfo['project_base'], self.pinfo['project_info'])
        if os.path.exists(os.path.join(os.getcwd(), pinfo_file)):
            with open(os.path.join(os.getcwd(), pinfo_file), 'r') as info_file:
                self.pinfo = json.load(info_file)
            info_file.close()
            self.check_data_is_list()
            self.data = self.pinfo['data']
            self.graph = WikiGraph(self)
            # ToDo: Init all data objects when loaded is pointless. Only init when needed. CHECK IF THATS correct!
            # self.init_data()
            return True
        else:
            return False

    def save_project(self):
        pinfo_file = os.path.join(self.pinfo['project_base'], self.pinfo['project_info'])
        with open(pinfo_file, 'w') as info_file:
            json.dump(self.pinfo, info_file, sort_keys=True, indent=4)
        return

    def init_graph_object(self):

        return

    '''
    def init_data(self):
        for key, value in self.pinfo['data'].items():
            if key == 'parsed':
                data_path = self.pinfo['path']['parsed']
            elif key == 'graph':
                data_path = self.pinfo['path']['graph']
            elif key == 'gt_graph':
                data_path = self.pinfo['path']['gt_graph']
            self.data[key] = self.init_data_obj(value, key, data_path)
        return
    '''

    '''
    @staticmethod
    def init_data_obj(data, data_type, data_path):
        if data_type == 'parsed' or data_type == 'graph':
            obj = WikiData(data, data_type, data_path)
            return obj
        elif data_type == 'gt_graph':
            # TODO implement
            pass
    '''

    def add_parsed_data(self, page_info=None, revision_info=None, author_info=None, cat_data=None, link_data=None,
                        description=None, override=False):
        assert page_info is not None and revision_info is not None \
               and cat_data is not None and author_info is not None, 'Error: Required data is missing. ' \
                                                                     'Only link_data is optional.'
        print('Moving parsed data to its location in the project folder is not yet implemented. '
              'You need put it there manually.')

        # Check if parsed data is already in the project.
        if 'parsed' in self.pinfo['data'].keys() and not override:
            print('Parsed data has already been added to the project. You can pass override=True to replace '
                  'parsed data information. Be aware that everything will be replaced.')
            return False

        # TODO Check if files exist in the correct location
        if not self.check_files([page_info, revision_info, cat_data, link_data], self.pinfo['path']['parsed']):
            print('File do not exists. Make sure you passed the correct file names '
                  'and that they are the correct folder.')
            return False

        self.pinfo['data']['parsed'] = {}
        self.pinfo['data']['parsed']['page_info'] = page_info
        self.pinfo['data']['parsed']['revision_info'] = revision_info
        self.pinfo['data']['parsed']['author_info'] = author_info
        self.pinfo['data']['parsed']['cat_data'] = cat_data
        if link_data is not None:
            self.pinfo['data']['parsed']['link_data'] = link_data
        self.pinfo['data']['parsed']['description'] = description
        self.check_data_is_list()
        self.save_project()

        #self.data['parsed'] = self.init_data_obj(self.pinfo['data']['parsed'],
        #                                         'parsed', self.pinfo['path']['parsed'])
        return True

    def add_graph_data(self, nodes, edges, events, description='cats', override=False):
        print('Moving graph data to its location in the project folder is not yet implemented. '
              'You need put it there manually.')

        # Check if parsed data is already in the project.
        if 'graph' in self.pinfo['data'].keys() and not override:
            print('Graph data has already been added to the project. You can pass override=True to replace '
                  'graph data information. Be aware that everything will be replaced.')
            return False

        self.pinfo['data']['graph'] = {}
        self.pinfo['data']['graph']['nodes'] = nodes
        self.pinfo['data']['graph']['edges'] = edges
        self.pinfo['data']['graph']['events'] = events
        self.pinfo['data']['graph']['description'] = description
        self.check_data_is_list()
        self.save_project()

    def add_gt_graph(self, gt_file, gt_wiki_id_map, gt_type='main', source_location=None, source_nodes=None,
                     source_edges=None, source_events=None):

        if gt_type in self.pinfo['gt_graph'].keys():
            print('Error. GT Graph of this type has already been added to the project.')
            return False

        if source_location is None:
            source_location = self.pinfo['path']['graph']
        if source_nodes is None:
            source_nodes = self.pinfo['data']['graph']['nodes']
        if source_edges is None:
            source_edges = self.pinfo['data']['graph']['edges']
        if source_events is None:
            source_events = self.pinfo['data']['graph']['events']

        gt_location = os.path.join(self.pinfo['path']['gt_graph'], gt_type)

        results = {
            'gt_file': gt_file,
            'gt_wiki_id_map': gt_wiki_id_map,
            'location': gt_location,
            'source_nodes': source_nodes,
            'source_edges': source_edges,
            'source_events': source_events,
            'source_location': source_location
        }

        self.pinfo['gt_graph'][gt_type] = results
        self.save_project()

    def get_title(self):
        return self.pinfo['project_title']

    def get_description(self):
        return self.pinfo['project_description']

    def get_start_date(self):
        if 'start_date' in self.pinfo.keys():
            return self.pinfo['start_date']
        else:
            return 'No start date has been calculated yet. Use the method find_start_date(self).'

    def get_dump_date(self):
        if 'dump_date' in self.pinfo.keys():
            return self.pinfo['dump_date']
        else:
            return 'No dump date has been set yet. Use the method set_dump_date to set it..'

    def set_title(self, project_title):
        self.pinfo['project_title'] = project_title
        self.save_project()

    def set_description(self, project_description):
        self.pinfo['project_description'] = project_description
        self.save_project()

    def set_dump_date(self, dump_date):
        self.pinfo['dump_date'] = dump_date
        self.save_project()

    def update_parsed_data_desc(self, description):
        if 'parsed' not in self.pinfo['data'].keys():
            print('No parsed data has been added to the project yet.')
            return False
        self.pinfo['data']['parsed']['description'] = description
        self.save_project()
        return True

    def find_start_date(self):
        if 'graph' in self.pinfo['data'].keys():
            self.pinfo['start_date'] = OldestRevision(self).get()
            self.save_project()
        else:
            print('The calculation of the start date requires graph data')
        pass

    def update_data_description(self, data_type, description):
        # TODO: Needs checking if correct
        self.pinfo['data'][data_type]['description'] = description
        self.save_project()
        pass

    def register_results_errors(self, data):
        if 'errors' in self.pinfo['data'].keys():
            for key, value in data.items():
                self.pinfo['data']['errors'][key] = value
        else:
            self.pinfo['data']['errors'] = data
        self.save_project()

    def update_gt_graph_data(self, data): #gt_type,
        # TODO: Needs checking if correct
        self.pinfo['gt_graph'] = data #[gt_type]
        self.save_project()
        pass

    def check_files(self, files, path):
        if type(files) == str:
            exists = self.check_single_file(files, path)
            return exists
        if type(files) == list:
            exists = True
            for f in files:
                if type(f) == str and exists:
                    exists = self.check_single_file(f, path)
                elif type(f) == list and exists:
                    exists = self.check_file_list(f, path)
            return exists

    @staticmethod
    def check_single_file(file, path):
        if os.path.exists(os.path.join(path, file)):
            exists = True
        else:
            exists = False
        return exists

    @staticmethod
    def check_file_list(file_list, path):
        exists = True
        for f in file_list:
            if not os.path.exists(os.path.join(path, f)):
                exists = False
        return exists

    def find_errors_in_parsed(self, data_type='cats', override=False):
        if 'errors' in self.pinfo['data'].keys() and not override:
            print('Error data has already been generated. Pass override=True to replace it.')
            return False
        if data_type == 'cats':
            #TODO AUSKOMMENTIEREN ENTFERNEN
            #ParsedDataErrorCheck(self, 'cat_data').missing_info_source_ids()
            ParsedDataErrorCheck(self, 'cat_data').find_unresolvable_target_titles()
        elif data_type == 'links':
            ParsedDataErrorCheck(self, 'link_data').missing_info_source_ids()
            ParsedDataErrorCheck(self, 'link_data').find_unresolvable_target_titles()
        elif data_type == 'all':
            ParsedDataErrorCheck(self, 'cat_data').missing_info_source_ids()
            ParsedDataErrorCheck(self, 'cat_data').find_unresolvable_target_titles()
            ParsedDataErrorCheck(self, 'link_data').missing_info_source_ids()
            ParsedDataErrorCheck(self, 'link_data').find_unresolvable_target_titles()
        else:
            print('No errors have been calculated. Pass cats, links or all to calculate them.')

    def generate_graph_data(self, data_type='cats'):
        processor = GraphDataGenerator(self)
        processor.generate_graph_data(data_type)

    def calculate_cscores(self):
        processor = ControvercyScore(self)
        processor.calculate()

    def generate_gt_graph(self):
        self.graph.generate_gt_graph()

    def check_data_is_list(self):
        if 'data' in self.pinfo.keys():
            for data_type, data in self.pinfo['data'].items():
                for field, info in data.items():
                    if field in ['cat_data', 'link_data', 'events', 'edges', 'nodes'] and type(info) == str:
                        self.pinfo['data'][data_type][field] = [info]
            self.save_project()

    # Funktioniert noch nicht. Außerdem: MACHT das hier Sinn? Oder doch lieber in graph objekt????
    def create_subgraph(self, title=None, seed=None, cats=True, subcats=1, supercats=1,
                        links=False, inlinks=1, outlinks=1):
        self.graph.create_subgraph(title=title, seed=seed, cats=cats, subcats=subcats, supercats=supercats,
                                   links=links, inlinks=inlinks, outlinks=outlinks)

    def create_gt_subgraph(self, title=None):
        self.graph.create_gt_subgraph(title=title)

    def create_snapshots(self, graph=None, title='Snapshots', slice='year', cscore=True,
                         start_date='2003-01-05', end_date=None):
        self.graph.create_snapshots(graph=graph, title=title, slice=slice, cscore=cscore,
                                    start_date=start_date, end_date=end_date)

    def remove_snapshots(self, subgraph, title):
        if subgraph in self.pinfo['gt_graph'].keys():
            if title in self.pinfo['gt_graph'][subgraph].keys():
                try:
                    shutil.rmtree(os.path.join(self.pinfo['gt_graph'][subgraph]['location'], title))
                except FileNotFoundError:
                    pass
                del self.pinfo['gt_graph'][subgraph][title]
                self.save_project()
            else:
                print('Snapshots do not exists')
        else:
            print('Subgraph does not exist')

    def remove_subgraph(self, subgraph):
        if subgraph in self.pinfo['gt_graph'].keys():
            try:
                shutil.rmtree(self.pinfo['gt_graph'][subgraph]['location'])
            except FileNotFoundError:
                pass
            del self.pinfo['gt_graph'][subgraph]
            self.save_project()
        else:
            print('Subgraph does not exist.')

    def create_static_viz(self, subgraph, layout, snapshots='snapshot_year', drawing_props_file=None, seed=None):
        if subgraph is not None:
            self.graph.set_working_graph(subgraph)
        if layout == 'ARF':
            ARF(self).snapshots(snapshots, drawing_props_file=drawing_props_file)
        elif layout == 'SFDP':
            SFDP(self).snapshots(snapshots, drawing_props_file=drawing_props_file)
        elif layout == 'RTL':
            RTL(self).snapshots(snapshots, drawing_props_file=drawing_props_file, seed=seed)
        elif layout == 'FR':
            FR(self).snapshots(snapshots, drawing_props_file=drawing_props_file)

    def get_highest_cscores(self, cscore_type, n=100, cats_only=False, save=True):
        self.graph.get_highest_cscores(cscore_type, n=n, cats_only=cats_only, save=save)


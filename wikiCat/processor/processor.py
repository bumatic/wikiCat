import os
import csv
import json


class Processor:
    def __init__(self, project, processor_type):
        self.project = project
        self.path = self.project.pinfo['path']

        if processor_type == 'parsed':
            self.data_path = self.project.pinfo['path']['parsed']
            self.results_path = self.project.pinfo['path']['graph']
            self.results_type = 'graph'
            self.page_info = self.project.pinfo['data']['parsed']['page_info']
            self.revision_info = self.project.pinfo['data']['parsed']['revision_info']
            self.author_info = self.project.pinfo['data']['parsed']['author_info']
            self.cat_data = self.project.pinfo['data']['parsed']['cat_data']
            if 'linked_data' in self.project.pinfo['data']['parsed'].keys():
                self.link_data = self.project.pinfo['data']['parsed']['link_data']
        elif processor_type == 'graph':
            self.data_path = self.project.pinfo['path']['graph']
            self.results_path = self.project.pinfo['path']['gt_graph']
            self.results_type = 'gt_graph'
        elif processor_type == 'gt_graph':
            self.data_path = self.project.pinfo['path']['gt_graph']
        else:
            print('Error. Please pass a valid processor_type: parsed, graph or gt_graph')
            return

        # TODO: STILL OLD: HOW TO HANDLE
        #if processor_type == 'gt_graph':
            # TODO this needs testing. probably a list of objs ist passed and not the obj
        #    self.data_obj = self.project.gt_graph_objs
        #else:
        #    self.data_obj = self.project.data_objs[processor_type] #oder _objs

    # ToDo: Needs adapting to new logic
    def register_graph_results(self, results_type, results): # , override=False

        # TODO Override muss eigentlich schon viel früher geklärt werden,
        # TODO da die Daten ja an der Stelle schon überschrieben sind.

        #TODO funktioniert noch nicht für Graph Data Cat und Link
        self.project.pinfo['data'][results_type] = results
        self.project.save_project()

    def register_gt_results(self, results_type, results):
        self.project.pinfo['gt_graph'][results_type] = results
        self.project.save_project()



    '''
    OLD
    def register_results(self, results_type, page_info=None, revision_info=None, cat_data=None, link_data=None,
                         error_data=None, error_type='error', nodes=None, edges=None, events=None, fixed='fixed_none',
                         errors='errors_removed', gt_file=None, gt_type='fixed_none__errors_removed', gt_wiki_id_map=None,
                         gt_source="graph__fixed_none__errors_removed", override=False):

        self.project.add_data(results_type, page_info=page_info, revision_info=revision_info, cat_data=cat_data,
                              link_data=link_data, error_data=error_data, error_type=error_type, nodes=nodes,
                              edges=edges, events=events, fixed=fixed, errors=errors, gt_file=gt_file,
                              gt_type=gt_type, gt_wiki_id_map=gt_wiki_id_map, gt_source=gt_source, override=override)
    '''

    @staticmethod
    def write_list(file, list_data):
        with open(file, 'w', newline='') as outfile:
            for item in list_data:
                outfile.write('%s\n' % '\t'.join(map(str, item)))
        return

    @staticmethod
    def write_json(file, dict_data):
        with open(file, 'w') as outfile:
            json.dump(dict_data, outfile, sort_keys=True, indent=4)
        return

    def remove_old(self, data_type):
        for files in self.project.pinfo.data[data_type].keys():
            if type(files) == str:
                os.remove(os.path.join(self.project.pinfo['path'][data_type], files))
            if type(files) == list:
                for f in files:
                    os.remove(os.path.join(self.project.pinfo['path'][data_type], f))

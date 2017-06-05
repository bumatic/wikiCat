import os
import csv
import json

class Processor:
    def __init__(self, project, processor_type):
        self.project = project
        self.path = self.project.pinfo['path']
        self.data_obj = self.project.data_objs[processor_type] #oder _objs
        if processor_type == 'dump':
            self.data_path = self.project.dump_data_path
            self.results_path = self.project.parsed_data_path
            self.results_type = 'parsed'
        elif processor_type == 'parsed':
            self.data_path = self.project.parsed_data_path
            self.results_path = self.project.graph_data_path
            self.results_type = 'graph'
            self.page_info = self.data_obj.data['page_info'][0][0]
            self.revision_info = self.data_obj.data['revision_info'][0][0]
            self.cat_data = self.data_obj.data['cat_data']
            self.link_data = self.data_obj.data['link_data']
        elif processor_type == 'graph':
            self.data_path = self.project.graph_data_path
            self.results_path = self.project.gt_graph_path
            self.results_type = 'graph'
        elif processor_type == 'gt_graph':
            self.data_path = self.project.gt_graph_path
        else:
            print('Error. Please pass a valid processor_type: dump, parsed, graph')
            return

    def write_list(self, file, list_data):
        with open(file, 'w', newline='') as outfile:
            for item in list_data:
                outfile.write('%s\n' % '\t'.join(map(str, item)))
        return

    def write_json(self, file, dict_data):
        with open(file, 'w') as outfile:
            json.dump(dict_data, outfile, sort_keys=True, indent=4)
        return

    def register_results(self, results_type, page_info=None, revision_info=None, cat_data=None, link_data=None,
                         error_data=None, error_type='error', nodes=None, edges=None, events=None, fixed='fixed_none',
                         errors='errors_removed', gt_file=None, gt_type='fixed_none__errors_removed', gt_wiki_id_map=None,
                         gt_source="graph__fixed_none__errors_removed", override=False):

        self.project.add_data(results_type, page_info=page_info, revision_info=revision_info, cat_data=cat_data,
                              link_data=link_data, error_data=error_data, error_type=error_type, nodes=nodes,
                              edges=edges, events=events, fixed=fixed, errors=errors, gt_file=gt_file,
                              gt_type=gt_type, gt_wiki_id_map=gt_wiki_id_map, gt_source=gt_source, override=override)


        '''
        if results_type == 'parsed':
            self.project.add_data('parsed', page_info=page_info, revision_info=revision_info, cat_data=cat_data,
                                  link_data=link_data, override=override)
        elif results_type == 'graph':
            self.project.add_data('graph', nodes=nodes, edges=edges, events=events, gt=gt, fixed=fixed, errors=errors,
                                  override=override)
            pass
        elif results_type == 'gt_graph':
            self.project.add_data()
        #TODO INCLUdE REGISTER GTGraph -> send to all
        elif results_type == 'error':
            self.project.add_data('error', error_data=error_data, error_type=error_type)
        elif results_type == 'results':
            pass
        '''



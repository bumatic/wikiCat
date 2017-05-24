import os


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
            self.results_path = self.project.graph_data_path
            self.results_type = 'graph'
        else:
            print('Error. Please pass a valid processor_type: dump, parsed, graph')
            return
    def write_list(self, file, list_data):
        with open(file, 'w', newline='') as outfile:
            wr = csv.writer(outfile, delimiter='\t', quoting=csv.QUOTE_NONE)
            wr.writerow(list_data)
        return

    def register_results(self, results_type, page_info=[], revision_info=[], cat_data=[], link_data=[],
                         error_data=[], error_type='error', nodes=[], edges=[], events=[],  fixed='fixed_none',
                         errors='errors_removed', override=False):
        if results_type == 'parsed':
            self.project.add_data('parsed', page_info=page_info, revision_info=revision_info, cat_data=cat_data,
                                  link_data=link_data, override=override)
        elif results_type == 'graph':
            self.project.add_data('graph', nodes=nodes, edges=edges, events=events, fixed=fixed, errors=errors,
                                  override=override)
            pass
        elif results_type == 'error':
            self.project.add_data('error', error_data=error_data, error_type=error_type)
        elif results_type == 'results':
            pass


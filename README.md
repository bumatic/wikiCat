
## How to: Basic Operations

### Import wikiCat

```
from wikiCat.wikiproject import Project
```

### Create Project
To create a new default project you need to pass init=True when calling Project(). If no project_path is provided, the project is created in the default path. Optionally you can set the project_path, project_info, title, description and dump_date.

```
mp = Project(init=True) 
```

### Load Project
To load an existing project. If no path is passed the script assumes that the project is located in the default path. In case you create a project in a non default path you need to pass the project_path. If this location is not correct you will get an error message.  

```
mp = Project()
'''

### Add Parsed Data (created with wikiDumpParser)

Adding parsed data to an existing project requires:

```
mp.add_parsed_data(page_info='page_info.csv',
                   revision_info='revisions.csv',
                   cat_data='cats.csv',
                   description='Some description about the data.')
```

### Adding graph data to a project
```
mp.add_graph_data('cat_data_fixed_none_errors_removed_1_nodes.csv', 'cat_data_fixed_none_errors_removed_1_edges.csv', 'cat_data_fixed_none_errors_removed_1_events.csv')
```

### Adding main GT Graph to the project
```
mp.add_gt_graph('gt_graph.gt', 'gt_nodes_graph.csv')
```

### Generate graph data from parsed data
```
mp.generate_graph_data(data_type='cats')
```

### Find errors in parsed data

```
mp.find_errors_in_parsed()
```

### Find first revision in data and use as start date for the project
```
mp.find_start_date()
```

### Calculate Cscores
```
mp.calculate_cscores()
```

### Generate gt_graph from graph data
```
mp.generate_gt_graph()
```

### Accessing gt_graph
```
mp.graph
```

### Listing available gt_graphs:
```
mp.graph.list_graphs()
```

### Setting working graph
```
mp.graph.set_working_graph('main')
```

### Get current working graph
```
mp.graph.get_working_graph()
```

### Create Subgraph
```
#mp.graph.set_working_graph('main')
mp.create_subgraph(title='Simulation_1_sub_1_super_gt', seed=[7744777], cats=True,
                   subcats=1, supercats=1)
```

### Create Snapshots
```
mp.create_snapshots(graph='Simulation_1_sub_1_super_gt', title='snapshots_year',
                    start_date='2003-01-10')
```

### Remove Snapshots
```
mp.remove_snapshots(subgraph='Simulation_1_sub_1_super_gt', title='Simulation_1_sub_1_super_gt')
```

### Remove Subgraph
```
mp.remove_subgraph(subgraph= ...) #untested
```

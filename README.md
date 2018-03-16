# pair

Cooper Pair is a Python library to simplify programmatic access to the DQM
GraphQL API. It is primarily intended to help us dogfood DQM services and
integrate DQM into our contract workflows (both Jupyter-based and batch),
and secondarily (and prospectively) as a resource for clients who want to write
their own code against DQM directly (perhaps as part of an Airflow graph).

## Installation

    pip install git+ssh://git@github.com/superconductive/cooper.git#egg=cooper_pair&subdirectory=pair

## Usage

### Instantiating the API

    from cooper_pair import CooperPair
    pair = CooperPair()

### Creating a new checkpoint

### Adding a new dataset

From a file:

    with open(filename, 'rb') as fd:
        dataset = pair.add_dataset_from_file(
            fd, project_id=project_id, created_by_id=created_by_id)
    dataset_id = dataset['dataset']['id']

### Creating a new checkpoint by autoinspection

    checkpoint = pair.add_checkpoint(checkpoint_name, autoinspect=True, dataset_id=dataset_id)
    checkpoint_id = checkpoint['addCheckpoint']['checkpoint']['id']
 
### Creating a new checkpoint from JSON
    
    import json
    with open('checkpoint_definition.json', 'rb') as fd:
        checkpoint_config = json.load(fd)

    pair.add_checkpoint_from_expectations_config(
        checkpoint_config, "Checkpoint Name")

### Evaluating a checkpoint on a dataset

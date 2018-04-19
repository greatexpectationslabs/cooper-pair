# pair

`cooper_pair` is a Python library that provides programmatic access to Superconductive's GraphQL API.

It supports a limited number of common use cases. (See below.)
`cooper_pair` is *not* intended as a general-purpose integration library for GraphQL.
Most useful GraphQL queries are *not* supported within the `cooper_pair` API.

## Why limit the use cases?

GraphQL is a composable query language. The space of allowed queries is enormous, and
developers are empowered to choose the right query for a given job. This de-couples development
behind the API from development that consumes the API, and allows each to move faster,
independently.

Wrapping a flexible GraphQL API in a rigid python library would completely defeat that purpose.

Instead, think of `cooper_pair` as training wheels. It makes it easy to quickly connect
to GraphQL, and perform a few common functions. It also provides a collection of example
queries to learn how to use GraphQL and the Allotrope API.

In other words, `cooper-pair` can help you get started, but you will be able to get far more
out of Allotrope once you learn to query it natively using GraphQL.

## Installation

    cd cooper-pair
    pip install .

Or,

    pip install git+ssh://git@github.com/superconductive/cooper.git#egg=cooper_pair&subdirectory=pair

## Usage

### Instantiate the API

    from cooper_pair import CooperPair
    pair = CooperPair()

### Adding a new dataset

From a file:

    with open(filename, 'rb') as fd:
        dataset = pair.add_dataset_from_file(
            fd,
            project_id=project_id
        )
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

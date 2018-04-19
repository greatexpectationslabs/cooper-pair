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

    pair = CooperPair(
        graphql_endpoint="http://my-data-valet-url:3010/graphql",
        email='my_user@some_email.com',
        password='my_very_secure_password'
    )

### List datasets

    response = pair.list_datasets()
    print( json.dumps(response, indent=2))

### Get a dataset
    response = pair.get_dataset("RGF2YXNldPoxODl=")
    print( json.dumps(response, indent=2))

### List checkpoints

    response = pair.list_checkpoints()
    print( json.dumps(response, indent=2) )

### Create a new dataset and evaluate it against an existing checkpoint

From a dataframe:

    my_df = pd.DataFrame({
        "x" : [1,2,3,4,5],
        "y" : [6,7,8,9,10],
    })
    response = pair.evaluate_checkpoint_on_pandas_df(
        checkpoint_id="Q2hlY2twb2ludDox",
        pandas_df=my_df,
        filename="my_dataframe_name"
    )
    evaluation_id = response['addEvaluation']['evaluation']['id']
    dataset_id = response['addEvaluation']['evaluation']['dataset']['id']

From a file:

    with open('my_file.csv', 'rb') as fd:
        dataset = pair.evaluate_checkpoint_on_file(
            checkpoint_id="Q2hlY2twb2ludDox",
            fd=fd,
        )
    evaluation_id = response['addEvaluation']['evaluation']['id']
    dataset_id = response['addEvaluation']['evaluation']['dataset']['id']

Note: Evaluation is asynchronous. When the response first comes back from Allotrope,
it will have `status="created"`. This will change to `pending` when a worker picks it up,
then to `success` or `failed` depending on the result of the evaluation.

You can query for status as follows:

    response = pair.query("""
            query evaluationQuery($id: ID!) {
                evaluation(id: $id) {
                    id,
                    status
                }
            }
        """,
        variables={
            'id' : evaluation_id
    })
    print(response)

### Creating a new checkpoint from JSON
    
    import json
    with open('checkpoint_definition.json', 'rb') as fd:
        checkpoint_config = json.load(fd)

    pair.add_checkpoint_from_expectations_config(
        checkpoint_config, "Checkpoint Name")



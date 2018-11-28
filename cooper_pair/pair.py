# pylint: disable=C0103, E0401, R0201
"""cooper_pair is a small library for programmatic access to the DQM
GraphQL API."""

import json
import os
import tempfile
import time
try:  # pragma: nocover
    from urllib.parse import parse_qs
except ImportError:  # pragma: nocover
    from urlparse import parse_qs
import warnings
import requests
import great_expectations as ge
from gql import gql, Client
from gql.client import RetryError
from gql.transport.requests import RequestsHTTPTransport

TIMEOUT = 60

MAX_RETRIES = 10

DQM_GRAPHQL_URL = os.environ.get('DQM_GRAPHQL_URL')


def make_gql_client(transport=None, schema=None, retries=MAX_RETRIES,
                    timeout=TIMEOUT):
    client = None
    counter = 0
    while client is None and counter < retries:
        start_time = time.time()
        while ((time.time() - start_time) <= timeout) and client is None:
            try:
                client = Client(
                    transport=transport,
                    fetch_schema_from_transport=(schema is None),
                    schema=schema,
                    retries=retries)
            except (requests.ConnectionError, RetryError):
                warnings.warn('CooperPair failed to connect to allotrope...')
        counter += 1

    if client is None:
        raise Exception(
            'CooperPair failed to connect to '
            'allotrope {} times.'.format(retries))

    return client


def generate_slug(name):
    """Utility function to generate snake-case-slugs.

    Args:
        name (str) -- the name to convert to a slug

    Returns:
        A string slug.
    """
    # TODO: this functionality should move to the server
    return name.lower().replace(' ', '-')


class CooperPair(object):
    """Entrypoint to the API."""

    _client = None

    def __init__(
            self,
            email=None,
            password=None,
            graphql_endpoint=DQM_GRAPHQL_URL,
            timeout=TIMEOUT,
            max_retries=MAX_RETRIES):
        """Create a new instance of CooperPair.

        Kwargs:
            graphql_endpoint (str) -- The GraphQL endpoint to hit (default:
                the value of the DQM_GRAPHQL_URL environment variable).
            timeout (int) -- The number of seconds to wait for API responses
                before timing out (default: 10).
            max_retries (int) -- The number of times to retry API requests
                before failing (default: 10). The worst-case time an API call
                may take is (max_retries x timeout) seconds.

        Raises:
            AssertionError, if graphql_endpoint is not set and the
                DQM_GRAPHQL_URL environment variable is not set.

        Returns:
            A new instance of CooperPair
        """
        assert graphql_endpoint, \
            'CooperPair.init: graphql_endpoint was None and ' \
            'DQM_GRAPHQL_URL not set.'

        if not(email and password):
            warnings.warn(
                'CooperPair must be initialized with email and password '
                'in order to authenticate against the GraphQL api.')

        self.email = email
        self.max_retries = max_retries
        self.password = password
        self.timeout = timeout
        self.token = None
        self.transport = RequestsHTTPTransport(
            url=graphql_endpoint, use_json=True, timeout=timeout)

    @property
    def client(self):
        if self._client is None:
            self._client = make_gql_client(
                transport=self.transport,
                retries=self.max_retries,
                timeout=self.timeout)
            # FIXME(mattgiles): login needs to be thought through
            self.login()
        return self._client

    def login(self, email=None, password=None):
        if self.email is None or self.password is None:
            warnings.warn(
                'Instance credentials are not set. You must '
                'set instance credentials (self.email and self.password) '
                'in order to automatically authenticate against '
                'the GraphQL api.')

        email = email or self.email
        password = password or self.password
        if email is None or password is None:
            warnings.warn('Must provide email and password to login.')
            return False
        login_result = self.client.execute(
            gql("""
                mutation loginMutation($input: LoginInput!) {
                    login(input: $input) {
                    token
                    }
                }
            """),
            variable_values={
                'input': {
                    'email': email,
                    'password': password
                }
            })
        token = login_result['login']['token']
        if token:
            self.token = token
            self.transport.headers = dict(
                self.transport.headers or {}, **{'X-Fullerene-Token': token})
            return True
        else:
            warnings.warn(
                "Couldn't log in with email and password provided. "
                "Please try again")
            return False

    def query(self, query, variables=None, unauthenticated=False):
        """Workhorse to execute queries.

        Args:
            query (string) -- A valid GraphQL query. query will apply
                gql.gql on the string to generate a graphql.language.ast.Document.

        Kwargs:
            variables (dict) -- A Python dict containing variables to be
                passed along with the GraphQL query (default: None, no
                variables will be passed).

        Returns:
            A dict containing the parsed results of the query.
        """
        if not unauthenticated:
            if not self.token:
                warnings.warn(
                    'Client not authenticated. Attempting to authenticate '
                    'using stored credentials...')

        query_gql = gql(query)
        
        try:
            return self.client.execute(query_gql, variable_values=variables)
        except (requests.exceptions.HTTPError, RetryError):
            self.transport.headers = dict(
                self.transport.headers or {}, **{'X-Fullerene-Token': None})
            self._client = None
            return self.client.execute(query_gql, variable_values=variables)

    def munge_ge_evaluation_results(self, ge_results):
        '''
        Unpack the Great Expectations result object to match the semi-flattened
        structure used by Allotrope.
        :param ge_results: a list of result dicts returned by Great Expectations
        :return: a list of result dicts that can be consumed by Allotrope
        '''
        return [
            {
                'success': result['success'],
                'expectationId': result['expectation_id'],
                'expectationType': result['expectation_config']['expectation_type'],
                'expectationKwargs': json.dumps(result['expectation_config']['kwargs']),
            
                'raisedException': result['exception_info']['raised_exception'],
                'exceptionTraceback': result['exception_info']['exception_traceback'],
                # 'exceptionMessage': result['exception_info']['exception_message'], #FIXME: Allotrope needs a new DB field to store this in
            
                'summaryObj': (
                    json.dumps(result['result'])
                    if 'result' in result else json.dumps({})
                )
            }
            for result in ge_results]
    
    def get_evaluation(self, evaluation_id):
        """
        Query an evaluation by id
        :param evaluation_id: Evaluation id
        :return: Graphql query result containing Evaluation dict
        """
        return self.query("""
            query evaluationQuery($id: ID!) {
                evaluation(id: $id) {
                    id
                    statusOrdinal
                    checkpointId
                    checkpoint {
                        name
                    }
                    dataset {
                        id
                        label
                    }
                    createdBy {
                        id
                    }
                    organization {
                        id
                    }
                    updatedAt
                    results {
                        edges {
                            node {
                                id
                                success
                                summaryObj
                                expectationType
                                expectationKwargs
                                raisedException
                                exceptionTraceback
                                evaluationId
                                expectationId
                                statusOrdinal
                            }
                        }
                    }
                }
            }
            """,
            variables={'id': evaluation_id}
        )
    
    def add_evaluation(
            self,
            dataset_id=None,
            checkpoint_id=None,
            checkpoint_name=None,
            delay_evaluation=False,
            results=None,
            status_ordinal=None
    ):
        """Add a new evaluation.

        Args:
            dataset_id (int or str Relay id) -- The id of the dataset on which
                to run the evaluation.
            checkpoint_id (int or str Relay id) -- The id of the checkpoint to
                evaluate.
            checkpoint_name (str) -- The name of the checkpoint to evaluate
            delay_evaluation (bool) -- If True, evaluation of dataset will be delayed
            results (list) -- List of ge evaluation results
            status_ordinal (int) -- Status ordinal of evaluation
        Returns:
            A dict containing the parsed results of the mutation.
        """
        if not checkpoint_id and not checkpoint_name:
            raise ValueError('must provide checkpoint_id or checkpoint_name')
                
        return self.query("""
            mutation addEvaluationMutation($evaluation: AddEvaluationInput!) {
                addEvaluation(input: $evaluation) {
                evaluation {
                    id
                    datasetId
                    dataset {
                        id
                        label
                        locatorDict
                    }
                    checkpointId
                    checkpoint {
                        id
                        name
                    }
                    createdById
                    createdBy {
                        id
                    }
                    organizationId
                    organization {
                        id
                    }
                    results {
                        pageInfo {
                            hasNextPage
                            hasPreviousPage
                            startCursor
                            endCursor
                        }
                        edges {
                            cursor
                            node {
                                id
                                success
                                summaryObj
                                expectationType
                                expectationKwargs
                                raisedException
                                exceptionTraceback
                                evaluationId
                                statusOrdinal
                            }
                        }
                    }
                    statusOrdinal
                    updatedAt
                }
                }
            }
        """,
        variables={
            'evaluation': {
                'datasetId': dataset_id,
                'checkpointId': checkpoint_id,
                'checkpointName': checkpoint_name,
                'delayEvaluation': delay_evaluation,
                'results': results,
                'statusOrdinal': status_ordinal
            }
        })
    
    def evaluate_checkpoint_on_pandas_df(
            self,
            checkpoint_id,
            pandas_df,
            filename=None,
            project_id=None):
        """Evaluate a expectation_suite on a pandas.DataFrame.
        
        Args:
            checkpoint_id (int or str Relay id) -- The id of the checkpoint to
                evaluate.
            pandas_df (pandas.DataFrame) -- The data frame on which to
                evaluate the expectation_suite.
        
        Kwargs:
            filename (str) -- The filename to associate with the dataset
                (default: None, the name attribute of the pandas_df argument
                will be used).
            project_id (int or str Relay id) -- The id of the project to associate
                with the evaluation
        
        Returns:
            A dict representation of the evaluation.
        """
        
        
        dataset = self.add_dataset_from_pandas_df(
            pandas_df,
            project_id,
            filename=filename)
        return self.add_evaluation(
            dataset['dataset']['id'],
            checkpoint_id
        )

    def evaluate_checkpoint_on_file(
            self,
            checkpoint_id,
            fd,
            filename=None,
            project_id=None):
        """Evaluate a expectation_suite on a file.
        
        Args:
            checkpoint_id (int or str Relay id) -- The id of the checkpoint to
                evaluate.
            fd (file-like) -- A file descriptor or file-like object to
                evaluate, opened as 'rb'.
        
        Kwargs:
            filename (str) -- The filename to associate with the dataset
                (default: None, the name attribute of the pandas_df argument
                will be used).
            project_id (int or str Relay id) -- The id of the project to associate
                with the evaluation
        
        Returns:
            A dict representation of the evaluation.
        """
        dataset = self.add_dataset_from_file(
            fd,
            project_id,
            filename=filename)
        return self.add_evaluation(
            dataset['dataset']['id'],
            checkpoint_id
        )

    def evaluate_pandas_df_against_checkpoint(
            self,
            pandas_df,
            dataset_label,
            checkpoint_id=None,
            checkpoint_name=None):
        """
        Evaluate a Pandas DataFrame against a checkpoint
        
        :param pandas_df: (pandas.DataFrame) The data frame on which to
                evaluate the checkpoint.
        :param dataset_label: (str) a human-readable name to associate with
                the evaluated dataset
        :param checkpoint_id: (int or str Relay id) the id of the checkpoint
                to evaluate against
        :param checkpoint_name: (str) the name of the checkpoint to evaluate
                against
                
        :return: a Great Expectations result object, as returned by .validate method
        """
        if not checkpoint_id and not checkpoint_name:
            raise ValueError('must provide checkpoint_id or checkpoint_name')
        if not checkpoint_id:
            checkpoint_id = self.get_checkpoint_by_name(checkpoint_name)['checkpoint']['id']
        expectations_config = self.get_checkpoint_as_expectations_config(
            checkpoint_id=checkpoint_id, checkpoint_name=checkpoint_name)
        expectation_ids = expectations_config.pop('expectation_ids', [])
        
        ge_results = pandas_df.validate(
            expectations_config=expectations_config,
            result_format="SUMMARY",
            catch_exceptions=True)
        results = ge_results['results']
        
        for idx, expectation_id in enumerate(expectation_ids):
            results[idx]['expectation_id'] = expectation_id
        
        munged_results = self.munge_ge_evaluation_results(ge_results=results)
        new_dataset = self.add_dataset(project_id=1, label=dataset_label)
        new_dataset_id = new_dataset['addDataset']['dataset']['id']
        self.add_evaluation(
            dataset_id=new_dataset_id,
            checkpoint_id=checkpoint_id,
            delay_evaluation=True,
            results=munged_results
        )
        return ge_results
    
    def update_evaluation(self, evaluation_id, status_ordinal=None, results=None):
        """Update an evaluation.

        Args:
            evaluation_id (int or str Relay id) -- The id of the evaluation
                to update
            status_ordinal (int) -- The status ordinal of the evaluation, if any
                (default: None)
            results (list of dicts) -- The results, if any (default: None)

        Returns:
            A dict containing the parsed results of the mutation.
        """
        variables = {
            'updateEvaluation': {
                'id': evaluation_id
            }
        }
        if results is not None:
            variables['updateEvaluation']['results'] = results
            
        if status_ordinal is not None:
            variables['updateEvaluation']['statusOrdinal'] = status_ordinal

        return self.query("""
            mutation($updateEvaluation: UpdateEvaluationInput!) {
                updateEvaluation(input: $updateEvaluation) {
                    evaluation {
                        id
                        datasetId
                        checkpointId
                        createdById
                        createdBy {
                            id
                        }
                        dataset {
                            id
                            label
                            locatorDict
                        }
                        organizationId
                        organization {
                            id
                        }
                        checkpoint {
                            id
                            name
                        }
                        results {
                            pageInfo {
                                hasNextPage
                                hasPreviousPage
                                startCursor
                                endCursor
                            }
                            edges {
                                cursor
                                node {
                                    id
                                    success
                                    summaryObj
                                    expectationType
                                    expectationKwargs
                                    raisedException
                                    exceptionTraceback
                                    evaluationId
                                    statusOrdinal
                                }
                            }
                        }
                        statusOrdinal
                        updatedAt
                    }
                }
            }
            """, variables=variables)
        
    def delete_evaluation(self, evaluation_id):
        """Delete an evaluation (soft delete).

        Args:
            evaluation_id (int or str Relay id) -- The id of the evaluation
                to delete

        Returns:
            A dict containing the parsed results of the mutation.
        """
        variables = {
            'updateEvaluation': {
                'id': evaluation_id,
                'deleted': True
            }
        }

        return self.query("""
            mutation($updateEvaluation: UpdateEvaluationInput!) {
                updateEvaluation(input: $updateEvaluation) {
                    evaluation {
                        id
                        deleted
                        deletedAt
                        updatedAt
                    }
                }
            }
            """, variables=variables)

    def get_dataset(self, dataset_id):
        """Retrieve a dataset by its id.

        Args:
            dataset_id (int or str Relay id) -- The id of the dataset
                to fetch

        Returns:
            A dict representation of the dataset.
        """
        return self.query("""
            query datasetQuery($id: ID!) {
                dataset(id: $id) {
                    id
                    label
                    project {
                        id
                    }
                    createdBy {
                        id
                    }
                    locatorDict
                    organization {
                        id
                    }
                }
            }
            """,
            variables={'id': dataset_id}
        )

    def list_datasets(self):
        return self.query("""{
            allDatasets{
                edges {
                    node{
                        id
                        label
                        locatorDict
                    }
                }
            }
        }""")

    def add_dataset(self, project_id, filename=None, label=None):
        """Add a new dataset object.

        Users should probably not call this function directly. Instead,
        consider add_dataset_from_file or add_dataset_from_pandas_df.

        Args:
            filename (str) -- The filename of the new dataset.
            project_id (int or str Relay id) -- The id of the project to which
                the dataset belongs.

        Returns:
            A dict containing the parsed results of the mutation.
        """
        return self.query("""
            mutation addDatasetMutation($dataset: AddDatasetInput!) {
                addDataset(input: $dataset) {
                    dataset {
                        id
                        label
                        project {
                            id
                        }
                        createdBy {
                            id
                        }
                        locatorDict
                        organization {
                            id
                        }
                    }
                }
            }
            """,
            variables={
                'dataset': {
                    'locatorDict': json.dumps({'filename': filename}) if filename else json.dumps({}),
                    'projectId': project_id,
                    'label': label
                }
            }
        )
        
    def add_dataset_simple(self, label, checkpoint_id, locator_dict, project_id=None):
        """
        Add a new Dataset object. Bypasses AddDataset mutation logic used for
        manually uploaded datasets
        :param label: (string) human readable identifier
        :param checkpoint_id: (int or string Relay id) id of checkpoint dataset belongs to
        :param locator_dict: (dict) dict containing data necessary for retrieving dataset. e.g.:
            {
                's3_bucket': '',
                's3_key': ''
            }
        :param project_id: (int or string Relay id, optional) id of project dataset belongs to
        :return: a dict representing the added Dataset
        """
        
        return self.query("""
            mutation addDatasetMutation($dataset: AddDatasetInput!) {
                addDataset(input: $dataset) {
                    dataset {
                        id
                        label
                        project {
                            id
                        }
                        createdBy {
                            id
                        }
                        locatorDict
                        organization {
                            id
                        }
                    }
                }
            }
            """,
            variables={
                'dataset': {
                    'checkpointId': checkpoint_id,
                    'locatorDict': json.dumps(locator_dict),
                    'label': label,
                    'projectId': project_id,
                    'simple': True
                }
            }
        )

    def add_dataset_from_file(
            self, fd, project_id, filename=None):
        """Add a new dataset from a file or file-like object.

        Args:
            fd (file-like) -- A file descriptor or file-like object to add
                as a new dataset, opened as 'rb'.
            project_id (int or str Relay id) -- The id of the project to which
                the dataset belongs.

        Kwargs:
            filename (str) -- The filename to associate with the dataset
                (default: None, the name attribute of the fd argument will be
                used). Note that in the case of file-like objects without
                names (e.g. py2 StringIO.StringIO), this must be set.

        Returns:
            A dict representation of the dataset.

        Raises:
            AttributeError, if filename is not set and fd does not have a
                name attribute.
        """
        dataset = self.add_dataset(
            project_id,
            filename or fd.name
        )

        presigned_post = dataset['addDataset']['dataset']['s3Url']

        self.upload_dataset(presigned_post, fd)

        return self.get_dataset(dataset['addDataset']['dataset']['id'])

    def add_dataset_from_pandas_df(
            self, pandas_df, project_id, filename=None):
        """Add a new dataset from a pandas.DataFrame.

        Args:
            pandas_df (pandas.DataFrame) -- The data frame to add.
            project_id (int or str Relay id) -- The id of the project to which
                the dataset belongs.

        Kwargs:
            filename (str) -- The filename to associate with the dataset
                (default: None, the name attribute of the pandas_df argument
                will be used).

        Returns:
            A dict representation of the dataset.

        Raises:
            AttributeError, if filename is not set and pandas_df does not have
                a name attribute.
        """
        with tempfile.TemporaryFile(mode='w+') as fd:
            pandas_df.to_csv(fd, encoding='UTF_8')
            fd.seek(0)
            return self.add_dataset_from_file(
                fd,
                project_id,
                filename=(filename or pandas_df.name)
            )

    def upload_dataset(self, presigned_post, fd):
        """Utility function to upload a file to S3.

        Users should probably not call this function directly. Instead,
        consider add_dataset_from_file or add_dataset_from_pandas_df.

        Args:
            presigned_post (str) -- A fully qualified presigned (POST) S3
                URL, including query string.
            fd (filelike) -- A file-like object opened for 'rb'.

        Returns:
            A requests.models.Response containing the results of the POST.
        """
        (s3_url, s3_querystring) = presigned_post.split('?')
        form_data = parse_qs(s3_querystring)
        return requests.post(s3_url, data=form_data, files={'file': fd})

    def delete_dataset(self, dataset_id):
        """Delete a dataset (soft delete).

        Args:
            dataset_id (int or str Relay id) -- The id of the dataset
                to delete

        Returns:
            A dict containing the parsed results of the mutation.
        """
        variables = {
            'updateDataset': {
                'id': dataset_id,
                'deleted': True
            }
        }

        return self.query("""
            mutation($updateDataset: UpdateDatasetInput!) {
                updateDataset(input: $updateDataset) {
                    dataset {
                        id
                        deleted
                        deletedAt
                        updatedAt
                    }
                }
            }
            """, variables=variables)
    
    def munge_ge_expectations_config(self, expectations_config):
        """
        Convert a Great Expectations expectations_config into a list
        of expectations that can be consumed by Checkpoints
        :param expectations_config: expectations_config dict as returned from
        Great Expectations
        :return: a list of parsed expectation dicts
        """
        expectations = expectations_config['expectations']
        munged_expectations = []
    
        for expectation in expectations:
            munged_expectations.append({
                'expectationType': expectation['expectation_type'],
                'expectationKwargs': json.dumps(expectation['kwargs'])
            })
    
        return munged_expectations

    def munge_ge_expectations_list(self, expectations):
        """
        Convert a Great Expectations expectation list to a list
        of expectations that can be consumed by Checkpoints
        :param expectations: a list of expectations as returned from
        Great Expectations
        :return: a list of parsed expectation dicts
        """
        munged_expectations = []
    
        for expectation in expectations:
            munged_expectations.append({
                'expectationType': expectation['expectation_type'],
                'expectationKwargs': json.dumps(expectation['kwargs'])
            })
    
        return munged_expectations

    def get_expectation_suite(self, expectation_suite_id):
        """Retrieve an existing expectation_suite.

        Args:
            expectation_suite_id (int or str Relay id) -- The id of the expectation_suite
                to retrieve

        Returns:
            A dict containing the parsed expectation_suite.
        """
        return self.query("""
            query expectationSuiteQuery($id: ID!) {
                expectationSuite(id: $id) {
                    id
                    autoinspectionStatus
                    organization {
                        id
                    }
                    expectations {
                        pageInfo {
                            hasNextPage
                            hasPreviousPage
                            startCursor
                            endCursor
                        }
                        edges {
                            cursor
                            node {
                                id
                                expectationType
                                expectationKwargs
                                isActivated
                                createdBy {
                                    id
                                }
                                organization {
                                    id
                                }
                                expectationSuite {
                                    id
                                }
                            }
                        }
                    }
                }
            }
            """,
            variables={'id': expectation_suite_id}
        )

    def get_expectation_suite_as_json_string(
            self, expectation_suite_id, include_inactive=False):
        """Retrieve a JSON representation of a expectation_suite.

        Args:
            expectation_suite_id (int or str Relay id) -- The id of the expectation_suite
                to retrieve
            include_inactive (bool) -- If true, evaluations whose isActivated
                flag is false will be included in the JSON config (default:
                False)

        Returns:
            A JSON representation of the expectation_suite.
        """
        expectation_suite = self.get_expectation_suite(expectation_suite_id)['expectationSuite']
        
        if include_inactive:
            expectations = [
                expectation['node']
                for expectation
                in expectation_suite['expectations']['edges']]
        else:
            expectations = [
                expectation['node']
                for expectation
                in expectation_suite['expectations']['edges']
                if expectation['node']['isActivated']]

        return json.dumps(
            {'expectations': [
                {
                    'expectation_type': expectation['expectationType'],
                    'kwargs': json.loads(expectation['expectationKwargs'])}
                for expectation in expectations]},
            indent=2,
            separators=(',', ': '),
            sort_keys=True)

    def get_expectation_suite_as_expectations_config(
            self, expectation_suite_id, include_inactive=False):
        """Retrieve an expectation suite  as a great_expectations expectations config.

        Kwargs:
            expectation_suite_id (int or str Relay id) -- The id of the expectation suite to
                retrieve
            include_inactive (bool) -- If true, expectations whose isActivated
                flag is false will be included in the JSON config (default:
                False).

        Returns:
            An expectations config dict as returned by
                great_expectations.dataset.DataSet.get_expectations_config.
        """
        expectation_suite = self.get_expectation_suite(expectation_suite_id)['expectationSuite']
    
        if include_inactive:
            expectations = [
                expectation['node']
                for expectation
                in expectation_suite['expectations']['edges']]
        else:
            expectations = [
                expectation['node']
                for expectation
                in expectation_suite['expectations']['edges']
                if expectation['node']['isActivated']]
        expectations_config = {
            'meta': {'great_expectations.__version__': '0.4.3'},
            'dataset_name': None,
            'expectations': [
                {'expectation_type': expectation['expectationType'],
                 'kwargs': json.loads(expectation['expectationKwargs'])}
                for expectation
                in expectations
            ]}
        return expectations_config

    def list_expectation_suites(self, complex=False):
        """Retrieve all existing expectation_suites.

        Returns:
            A dict containing the parsed query.
        """
        if not complex:
            return self.query("""
                query listExpectationSuiteQuery{
                    allExpectationSuites {
                        edges {
                            node {
                                id
                                name
                            }
                        }
                    }
                }
            """)
        else:
            return self.query("""
                query listExpectationSuiteQuery{
                    allExpectationSuites {
                        pageInfo {
                            hasNextPage
                            hasPreviousPage
                            startCursor
                            endCursor
                        }
                        edges {
                            cursor
                            node {
                                id
                                name
                                autoinspectionStatus
                                organization {
                                    id
                                }
                                expectations {
                                    pageInfo {
                                        hasNextPage
                                        hasPreviousPage
                                        startCursor
                                        endCursor
                                    }
                                    edges {
                                        cursor
                                        node {
                                            id
                                            expectationType
                                            expectationKwargs
                                            isActivated
                                            createdBy {
                                                id
                                            }
                                            organization {
                                                id
                                            }
                                            expectationSuite {
                                                id
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                """
            )

    def add_expectation_suite(self, name, autoinspect=False, dataset_id=None, expectations=None):
        """Add a new expectation_suite.

        Users should probably not call this function directly. Instead,
        consider add_expectation_suite_from_expectations_config.

        Args:
            name (str) -- The name of the expectation_suite to create.

        Kwargs:
            autoinspect (bool) -- Flag to populate the expectation_suite with
                single-column expectations generated by autoinspection of a
                dataset (default: false).
            dataset_id (int or str Relay id) -- The id of the dataset to
                autoinspect (default: None).
            expectations (list) -- A list of expectations to associate with
                the expectation_suite

        Raises:
            AssertionError if autoinspect is true and dataset_id is not
            present, or if dataset_id is present and autoinspect is false.

        Returns:
            A dict containing the parsed results of the mutation.
        """
        # TODO: implement nested object creation for addExpectationSuite
        if autoinspect:
            assert dataset_id, 'Must pass a dataset_id when autoinspecting.'
        else:
            assert dataset_id is None, 'Do not pass a dataset_id if not ' \
                'autoinspecting.'
        return self.query("""
            mutation addExpectationSuiteMutation($expectationSuite: AddExpectationSuiteInput!) {
                addExpectationSuite(input: $expectationSuite) {
                    expectationSuite {
                        id
                        name
                        slug
                        autoinspectionStatus
                        createdBy {
                        id
                        }
                        expectations {
                            pageInfo {
                                hasNextPage
                                hasPreviousPage
                                startCursor
                                endCursor
                            }
                            edges {
                                cursor
                                node {
                                    id
                                }
                            }
                        }
                        organization {
                        id
                        }
                    }
                }
            }
        """,
        variables={
            'expectationSuite': {
                'name': name,
                'slug': generate_slug(name),
                'autoinspect': autoinspect,
                'datasetId': dataset_id,
                'expectations': expectations
            }
        })
    
    def add_expectation_suite_from_expectations_config(
            self, expectations_config, name):
        """Create a new expectation_suite from a great_expectations expectations
            config.

        Args:
            expectations_config (dict) - An expectations config as returned by
                great_expectations.dataset.DataSet.get_expectations_config.
                Note that this is not validated here or on the server side --
                failures will occur at evaluation time.
            name (str) - The name of the expectation_suite to create.

        Returns:
            A dict containing the parsed expectation_suite.
        """
        expectations = self.munge_ge_expectations_config(expectations_config)
        return self.add_expectation_suite(name=name, expectations=expectations)

    def add_expectation_suite_from_ge_expectations_list(
            self, expectations_list, name):
        """Create a new expectation_suite from a great_expectations expectations
            list.

        Args:
            expectations_list (list) - A list of Great Expectations
                formatted expectations
                Note that this is not validated here or on the server side --
                failures will occur at evaluation time.
            name (str) - The name of the expectation_suite to create.

        Returns:
            A dict containing the parsed expectation_suite.
        """
        expectations = self.munge_ge_expectations_list(expectations_list)
        return self.add_expectation_suite(name=name, expectations=expectations)
    
    def update_expectation_suite(
        self,
        expectation_suite_id,
        autoinspection_status=None,
        expectations=None):
        """Update an existing expectation_suite.

        Args:
            expectation_suite_id (int or str Relay id) -- The id of the expectation_suite
                to update.

        Kwargs:
            autoinspection_status (str) -- The status of autoinspection, if
                that is to be updated (default: None, no change).
            expectations (list) -- A list of dicts representing expectations
                to be created & added to the expectation_suite (default: None,
                no change). Note: semantics are append.

        Returns:
            A dict representing the parsed results of the mutation.
        """
        assert any([
            autoinspection_status is not None,
            expectations is not None]), \
            'Must update one of autoinspection_status or expectations'

        variables = {
            'expectationSuite': {
                'id': expectation_suite_id
            }
        }

        if expectations is not None:
            variables['expectationSuite']['expectations'] = expectations
        if autoinspection_status is not None:
            variables['expectationSuite']['autoinspectionStatus'] = \
                autoinspection_status

        result = self.query("""
            mutation updateExpectationSuiteMutation($expectationSuite: UpdateExpectationSuiteInput!) {
                updateExpectationSuite(input: $expectationSuite) {
                    expectationSuite {
                        id
                        expectations {
                            pageInfo {
                                hasNextPage
                                hasPreviousPage
                                startCursor
                                endCursor
                            }
                            edges {
                                cursor
                                node {
                                    id
                                    expectationType
                                    expectationKwargs
                                    isActivated
                                    createdBy {
                                        id
                                    }
                                    organization {
                                        id
                                    }
                                    expectationSuite {
                                        id
                                    }
                                }
                            }
                        }
                    }
                }
            }
            """,
            variables=variables
        )
        return result

    def get_expectation(self, expectation_id):
        """Retrieve an expectation by its id.

        Args:
            expectation_id (int or str Relay id) -- The id of the expectation
                to fetch

        Returns:
            A dict representation of the expectation.
        """
        return self.query("""
            query expectationQuery($id: ID!) {
                expectation(id: $id) {
                    id
                    expectationType
                    expectationKwargs
                    isActivated
                    createdBy {
                        id
                    }
                    organization {
                        id
                    }
                    expectationSuite {
                        id
                    }
                }
            }
            """,
            variables={'id': expectation_id}
        )
    
    def add_expectation(
            self,
            expectation_suite_id,
            expectation_type,
            expectation_kwargs,
        ):
        """Add a new expectation to an expectation_suite.

        Args:
            expectation_suite_id (int or str Relay id) -- The id of the expectation_suite
                to which to add the new expectation.
            expectation_type (str) -- A valid great_expectations expectation
                type. Note: these are not yet validated by client or
                server code, so failures will occur at evaluation time.
            expectation_kwargs (JSON dict) -- Valid great_expectations
                expectation kwargs, as JSON. Note: these are not yet validated
                by client or server code, so failures will occur at evaluation
                time.

        Returns:
            A dict containing the parsed results of the mutation.

        Raises:
            ValueError, if expectation_kwargs are not parseable as JSON
        """
        # TODO: use common code (JSON schema) to validate expectation_type and
        # expectation_kwargs
        try:
            json.loads(expectation_kwargs)
        except (TypeError, ValueError):
            raise ValueError(
                'Must provide valid JSON expectation_kwargs (got %s)',
                expectation_kwargs)

        return self.query("""
            mutation addExpectationMutation($expectation: AddExpectationInput!) {
                addExpectation(input: $expectation) {
                expectation {
                    id
                    expectationType
                    expectationKwargs
                    isActivated
                    createdBy {
                        id
                    }
                    organization {
                        id
                    }
                    expectationSuite {
                        id
                    }
                }
                }
            }
        """,
        variables={
            'expectation': {
                'expectationSuiteId': expectation_suite_id,
                'expectationType': expectation_type,
                'expectationKwargs': expectation_kwargs,
        }})

    def update_expectation(
            self,
            expectation_id,
            expectation_type=None,
            expectation_kwargs=None,
            is_activated=None):
        # TODO: use common code (JSON schema) to validate expectation_type and
        # expectation_kwargs
        """Update an existing expectation.

        Args:
            expectation_id (int or str Relay id) -- The id of the expectation
                to update.

        Kwargs:
            expectation_type (str) -- A valid great_expectations expectation
                type (default: None, no change). Note: these are not yet
                validated by client or server code, so failures will occur at
                evaluation time.
            expectation_kwargs (str) -- Valid great_expectations
                expectation kwargs, as JSON (default: None, no change).
                If present, the existing expectation_kwargs will be
                overwritten, so updates must include all unchanged keys from
                the existing kwargs. Note: these are not yet validated by
                client or server code, so failures will occur at evaluation
                time..
            is_activated (bool) -- Flag indicating whether an expectation
                should be evaluated (default: None, no change).

        Returns:
            A dict containing the parsed results of the mutation.

        Raises:
            AssertionError, if none of expectation_type, expectation_kwargs,
                or is_activated is provided
            ValueError, if expectation_kwargs are provided but not parseable
                as JSON
        """
        assert any([
            expectation_type is not None,
            expectation_kwargs is not None,
            is_activated is not None]), 'Must provide expectation_type, ' \
            'expectation_kwargs, or is_activated flag'
        if expectation_kwargs:
            try:
                json.loads(expectation_kwargs)
            except (TypeError, ValueError):
                raise ValueError(
                    'Must provide valid JSON expectation_kwargs (got %s)',
                    expectation_kwargs)

        variables = {
            'expectation': {'id': expectation_id}}
        if is_activated is not None:
            variables['expectation']['isActivated'] = is_activated
        if expectation_type is not None:
            variables['expectation']['expectationType'] = expectation_type
        if expectation_kwargs is not None:
            variables['expectation']['expectationKwargs'] = expectation_kwargs

        return self.query("""
            mutation updateExpectationMutation($expectation: UpdateExpectationInput!) {
                updateExpectation(input: $expectation) {
                expectation {
                    id
                    expectationType
                    expectationKwargs
                    isActivated
                    createdBy {
                        id
                    }
                    organization {
                        id
                    }
                    expectationSuite {
                        id
                    }
                }
                }
            }
            """,
            variables=variables
        )

    def get_checkpoint(self, checkpoint_id):
        """Retrieve an existing checkpoint.

        Args:
            checkpoint_id (int or str Relay id) -- The id of the checkpoint
                to retrieve

        Returns:
            A dict containing the parsed checkpoint.
        """
        return self.query("""
            query checkpointQuery($id: ID!) {
                checkpoint(id: $id) {
                    id
                    name
                    slug
                    isActivated
                    createdBy {
                        id
                        firstName
                        lastName
                        email
                    }
                    expectationSuite {
                        expectations {
                            pageInfo {
                                hasNextPage
                                hasPreviousPage
                                startCursor
                                endCursor
                            }
                            edges {
                                cursor
                                node {
                                    id
                                    expectationType
                                    expectationKwargs
                                    isActivated
                                    createdBy {
                                        id
                                    }
                                    organization {
                                        id
                                    }
                                }
                            }
                        }
                    }
                }
            }
            """,
                          variables={'id': checkpoint_id}
                          )

    def get_checkpoint_by_name(self, checkpoint_name):
        """Retrieve an existing checkpoint by name.

        Args:
            name (str) -- The name of the checkpoint
                to retrieve

        Returns:
            A dict containing the parsed checkpoint.
        """
        return self.query("""
            query checkpointQuery($name: String!) {
                checkpoint(name: $name) {
                    id
                    name
                    slug
                    isActivated
                    createdBy {
                        id
                        firstName
                        lastName
                        email
                    }
                    expectationSuite {
                        expectations {
                            pageInfo {
                                hasNextPage
                                hasPreviousPage
                                startCursor
                                endCursor
                            }
                            edges {
                                cursor
                                node {
                                    id
                                    expectationType
                                    expectationKwargs
                                    isActivated
                                    createdBy {
                                        id
                                    }
                                    organization {
                                        id
                                    }
                                }
                            }
                        }
                    }
                }
            }
            """,
                          variables={'name': checkpoint_name}
                          )

    def get_checkpoint_as_expectations_config(
            self, checkpoint_id=None, checkpoint_name=None, include_inactive=False):
        """Retrieve a checkpoint as a great_expectations expectations config.

        Kwargs:
            checkpoint_id (int or str Relay id) -- The id of the checkpoint to
                retrieve
            checkpoint_name (str) -- The name of the checkpoint to retrieve
            include_inactive (bool) -- If true, evaluations whose isActivated
                flag is false will be included in the JSON config (default:
                False).

        Returns:
            An expectations config dict as returned by
                great_expectations.dataset.DataSet.get_expectations_config.
        """
        if not checkpoint_id and not checkpoint_name:
            raise ValueError('must provide checkpoint_id or checkpoint_name')
        
        if checkpoint_id:
            checkpoint = self.get_checkpoint(checkpoint_id)
        else:
            checkpoint = self.get_checkpoint_by_name(checkpoint_name)
    
        if include_inactive:
            expectations = [
                expectation['node']
                for expectation
                in checkpoint['checkpoint']['expectationSuite']['expectations']['edges']]
        else:
            expectations = [
                expectation['node']
                for expectation
                in checkpoint['checkpoint']['expectationSuite']['expectations']['edges']
                if expectation['node']['isActivated']]
        expectation_ids = [expectation['id'] for expectation in expectations]
        expectations_config = {
            'meta': {'great_expectations.__version__': '0.4.4'},
            'expectation_ids': expectation_ids,
            'dataset_name': None,
            'expectations': [
                {'expectation_type': expectation['expectationType'],
                 'kwargs': json.loads(expectation['expectationKwargs'])}
                for expectation
                in expectations
            ]}
        return expectations_config

    def list_checkpoints(self):
        """
        Retrieve all existing checkpoints
        :return: A dict containing all checkpoints. Ex.:
        {
            'allCheckpoints': {
                'edges': [
                    {
                        'node': {
                            "id": "Q2hlY2twb2ludDox",
                            "name": "Claims by Race and Gender-UploadCheckpoint",
                            "tableName": "Humana",
                            "slug": "claims-by-race-and-gender-upload-checkpoint",
                            "isActivated": true,
                            "createdById": 1,
                            "organizationId": 1,
                            "projectId": 1,
                            "expectationSuiteId": 3,
                            "sensorId": 1
                        }
                    }, ...
                ]
            }
        }
        """
        return self.query("""
            query {
                allCheckpoints {
                    edges {
                        node {
                            id
                            name
                            tableName
                            slug
                            isActivated
                            createdById
                            organizationId
                            projectId
                            expectationSuiteId
                            sensorId
                            sensor {
                                type
                            }
                        }
                    }
                }
            }
        """)

    def add_checkpoint(
            self,
            name,
            table_name=None,
            is_activated=True,
            slack_webhook=None,
            expectation_suite_id=None,
            sensor_id=None):
        """
        Add a checkpoint.
        :param name: Name of checkpoint
        :param table_name: Name of associated table
        :param is_activated: boolean
        :param slack_webhook: optional slack webhook address to create
            condigured_notification on checkpoint creation
        :param expectation_suite_id: The id of corresponding expectation suite
        :param sensor_id: The id of corresponding sensor
        :return: A dict with parsed checkpoint (see query for structure)
        """

        return self.query("""
            mutation addCheckpointMutation($checkpoint: AddCheckpointInput!) {
                addCheckpoint(input: $checkpoint) {
                    checkpoint {
                        id
                        name
                        tableName
                        slug
                        isActivated
                        sensor {
                            id
                        }
                        expectationSuite {
                            id
                        }
                        createdBy {
                            id
                        }
                        organization {
                            id
                        }
                        configuredNotifications {
                            pageInfo {
                                hasNextPage
                                hasPreviousPage
                                startCursor
                                endCursor
                            }
                            edges {
                                cursor
                                node {
                                    id
                                }
                            }
                        }
                    }
                }
            }
            """,
            variables={
                'checkpoint': {
                    'name': name,
                    'tableName': table_name,
                    'slug': generate_slug(name),
                    'isActivated': is_activated,
                    'expectationSuiteId': expectation_suite_id,
                    'sensorId': sensor_id,
                    'slackWebhook': slack_webhook
                }
            }
        )

    def setup_checkpoint_from_ge_expectations_config(
            self, checkpoint_name, expectations_config, slack_webhook=None):
        
        """
        First creates a new expectation suite, which generates new default checkpoint, sensor,
        and datasource for manual file upload. After a new expectation suite is created, the new
        checkpoint is created (and optionally, a new Slack notification)
        :param checkpoint_name: (str) the name of the checkpoint to be created
        :param expectations_config: (dict) an expectation config as return by Great Expectations
        :param slack_webhook: (str) a Slack webhook to route notifications to
        :return: the dict representation of the checkpoint that was created
        """
        new_expectation_suite = self.add_expectation_suite_from_expectations_config(
            name=checkpoint_name, expectations_config=expectations_config)
        new_expectation_suite_id = new_expectation_suite['addExpectationSuite']['expectationSuite']['id']
        return self.add_checkpoint(
            name=checkpoint_name, expectation_suite_id=new_expectation_suite_id, slack_webhook=slack_webhook
        )
        
    def setup_checkpoint_from_ge_expectations_list(
            self, checkpoint_name, expectations_list, slack_webhook=None):
        """
        First creates a new expectation suite, which generates new default checkpoint, sensor,
        and datasource for manual file upload. After a new expectation suite is created, the new
        checkpoint is created (and optionally, a new Slack notification)
        :param checkpoint_name: (str) the name of the checkpoint to be created
        :param expectations_list: (list) an expectations list as return by Great Expectations
        :param slack_webhook: (str) a Slack webhook to route notifications to
        :return: the dict representation of the checkpoint that was created
        """
        new_expectation_suite = self.add_expectation_suite_from_ge_expectations_list(
            name=checkpoint_name, expectations_list=expectations_list)
        new_expectation_suite_id = new_expectation_suite['addExpectationSuite']['expectationSuite']['id']
        return self.add_checkpoint(
            name=checkpoint_name, expectation_suite_id=new_expectation_suite_id, slack_webhook=slack_webhook
        )

    def list_configured_notifications_on_checkpoint(self, checkpoint_id):
        """Retrieve all existing configured notifications for 
        a given checkpoint_id.

        Returns:
            A dict containing the parsed query.
        """

        return self.query("""
            query checkpointQuery($id: ID!) {
                checkpoint(id: $id) {
                    configuredNotifications {
                        edges {
                            node {
                                id
                                notificationType
                                value
                                notifyOn
                            }
                        }
                    }
                }
            }
            """, variables={'id': checkpoint_id})
    
    def add_sensor(self, name, type, data_source_id=None, excluded_paths=None, sensor_config=None):
        """
        Adds a new sensor.
        :param name: (str) name to identify sensor
        :param type: (str) type of sensor
        :param data_source_id: (int or str relay id) id of associated data source
        :param excluded_paths: (array of dicts) paths to exclude from evaluation on
        sensor execution, of form {'path': ..., 'reason': ...}
        :param sensor_config: (dict) configuration dict with info for specifying which
        files are evaluated and optionally, an s3 bucket to save file after evaluation,
        :return: (dict) a dict representation of added sensor
        """
        variables = {
            'sensor': {
                'name': name,
                'type': type
            }
        }
        
        if data_source_id:
            variables['sensor']['dataSourceId'] = data_source_id
        if excluded_paths:
            variables['sensor']['excludedPaths'] = json.dumps(excluded_paths)
        if sensor_config:
            variables['sensor']['sensorConfig'] = json.dumps(sensor_config)
        
        return self.query("""
            mutation addSensorMutation($sensor: AddSensorInput!) {
                addSensor(input: $sensor) {
                    sensor {
                        id
                        name
                        type
                        dataSourceId
                        createdBy {
                            id
                            firstName
                            lastName
                        }
                        organization {
                            id
                            name
                        }
                        excludedPaths
                        sensorConfig
                    }
                }
            }""",
            variables=variables
        )
        
    def update_sensor(self, sensor_id, name=None, data_source_id=None, excluded_paths=None, sensor_config=None):
        """
        Updates an existing sensor.
        :param sensor_id: (int or str relay id) id of sensor to update
        :param name: (str) name to identify sensor
        :param data_source_id: (int or str relay id) id of associated data source
        :param excluded_paths: (array of dicts) paths to exclude from evaluation on
        sensor execution, of form {'path': ..., 'reason': ...}
        :param sensor_config: (dict) configuration dict with info for specifying which
        files are evaluated and optionally, an s3 bucket to save file after evaluation,
        :return: (dict) a dict representation of updated sensor
        """
        variables = {
            'sensor': {
                'id': sensor_id
            }
        }

        if name:
            variables['sensor']['name'] = name
        if data_source_id:
            variables['sensor']['dataSourceId'] = data_source_id
        if excluded_paths:
            variables['sensor']['excludedPaths'] = json.dumps(excluded_paths)
        if sensor_config:
            variables['sensor']['sensorConfig'] = json.dumps(sensor_config)
            
        return self.query("""
            mutation updateSensorMutation($sensor: UpdateSensorInput!) {
                updateSensor(input: $sensor) {
                    sensor {
                        id
                        name
                        dataSourceId
                        createdBy {
                            id
                            firstName
                            lastName
                        }
                        organization {
                            id
                            name
                        }
                        excludedPaths
                        sensorConfig
                    }
                }
            }""",
            variables={
                'sensor': {
                    'id': sensor_id,
                    'name': name,
                    'dataSourceId': data_source_id,
                    'excludedPaths': excluded_paths,
                    'sensorConfig': sensor_config
                }
            }
        )

    def add_excluded_path_to_sensor(self, sensor_id, new_excluded_path_dict):
        return self.query("""
            mutation updateSensorMutation($sensor: UpdateSensorInput!) {
                updateSensor(input: $sensor) {
                    sensor {
                        id
                        excludedPaths
                    }
                }
            }
            """, variables={
                    'sensor': {
                        'id': sensor_id,
                        'newExcludedPathDict': json.dumps(new_excluded_path_dict)
                    }
                }
        )
    
    def trigger_sensor(self, sensor_id):
        return self.query("""
            mutation triggerSensorMutation($sensor: TriggerSensorInput!) {
                triggerSensor(input: $sensor) {
                    evaluationIds
                }
            }
            """, variables={
                    'sensor': {
                        'id': sensor_id,
                    }
                }
        )
    
    def add_data_source(self, name, type, is_activated=True, credentials_reference=None):
        """
        Adds a new data source.
        :param name: (str) name to identify data source
        :param type: (str) type of data source (i.e. 's3', 'database')
        :param is_activated: (bool) active status
        :param credentials_reference: (dict) dict configuration with info on how to
        connect to data source, e.g. {
            's3_staging_bucket': ...,
            'aws_access_key_id': ...,
            'aws_secret_access_key': ...,
            's3_bucket': ...,
            'prefix': ...
        }
        :return: (dict) a dict representation of the added data source
        """
        variables = {
            'dataSource': {
                'name': name,
                'type': type,
                'isActivated': is_activated,
            }
        }
        
        if credentials_reference:
            variables['dataSource']['credentialsReference'] = json.dumps(credentials_reference)
        
        return self.query("""
            mutation addDataSourceMutation($dataSource: AddDataSourceInput!) {
                addDataSource(input: $dataSource) {
                    dataSource {
                        id
                        name
                        type
                        isActivated
                        createdBy {
                            id
                            firstName
                            lastName
                        }
                        organization {
                            id
                            name
                        }
                        credentialsReference
                    }
                }
            }""",
            variables=variables
        )
        
    def update_data_source(
            self,
            data_source_id,
            name=None,
            type=None,
            is_activated=None,
            test_status=None,
            test_error_message=None,
            credentials_reference=None
    ):
        """
        Updates an existing data source
        :param data_source_id: (int or str relay id) id of data source to update
        :param name: (str) name to identify data source
        :param type: (str) type of data source (i.e. 's3', 'database')
        :param is_activated: (bool) active status
        :param test_status: (str) test status of data source (None, 'success', 'failed')
        :param test_error_message: (str) optional, error message of failed test
        :param credentials_reference: (dict) dict configuration with info on how to
        connect to data source, e.g. {
            's3_staging_bucket': ...,
            'aws_access_key_id': ...,
            'aws_secret_access_key': ...,
            's3_bucket': ...,
            'prefix': ...
        }
        :return: (dict) a dict representation of the added data source
        """
        variables = {
            'dataSource': {
                'id': data_source_id
            }
        }

        if name:
            variables['dataSource']['name'] = name
        if type:
            variables['dataSource']['type'] = type
        if is_activated or is_activated is False:
            variables['dataSource']['isActivated'] = is_activated
        if credentials_reference:
            variables['dataSource']['credentialsReference'] = json.dumps(credentials_reference)
        if test_status:
            variables['dataSource']['testStatus'] = test_status
        if test_error_message:
            variables['dataSource']['testErrorMessage'] = test_error_message
        
        return self.query("""
            mutation updateDataSourceMutation($dataSource: UpdateDataSourceInput!) {
                updateDataSource(input: $dataSource) {
                    dataSource {
                        id
                        name
                        type
                        isActivated
                        testStatus
                        testErrorMessage
                        createdBy {
                            id
                            firstName
                            lastName
                        }
                        organization {
                            id
                            name
                        }
                        credentialsReference
                    }
                }
            }""",
            variables=variables
        )

    def get_config_property_by_name(self, name):
        """Retrieve an existing checkpoint by name.

        Args:
            name (str) -- The name of the config property
                to retrieve

        Returns:
            The config property value.
        """
        config_property = self.query("""
            query configPropertyQuery($name: String!) {
                configProperty(name: $name) {
                    value
                }
            }
            """, variables={'name': name})['configProperty']
        
        if config_property:
            return config_property['value']
        else:
            return None

    def list_config_properties(self):
        return self.query("""{
            allConfigProperties{
                edges {
                    node{
                        id
                        name
                        value
                    }
                }
            }
        }""")
    
    def list_priority_levels(self):
        return self.query("""{
            allPriorityLevels {
                edges {
                    node {
                        id
                        level
                        ordinal
                        iconClassName
                        colorClassName
                    }
                }
            }
        }""")

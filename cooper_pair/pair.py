# pylint: disable=C0103, E0401, R0201
"""cooper_pair is a small library for programmatic access to the DQM
GraphQL API."""

import json
import os
import tempfile
import time
import traceback
try:  # pragma: nocover
    from urllib.parse import parse_qs
except ImportError:  # pragma: nocover
    from urlparse import parse_qs
import warnings

import requests

from gql import gql, Client
from gql.client import RetryError
from gql.transport.requests import RequestsHTTPTransport
from graphql import (parse, introspection_query, build_client_schema)


TIMEOUT = 2

MAX_RETRIES = 10

DQM_GRAPHQL_URL = os.environ.get('DQM_GRAPHQL_URL')


def make_gql_client(transport=None, schema=None, retries=MAX_RETRIES,
                    timeout=TIMEOUT):
    client = None
    counter = 0
    while client is None and counter < retries:
        try:
            client = Client(
                transport=transport,
                fetch_schema_from_transport=(schema is None),
                schema=schema,
                retries=retries)
        except (requests.ConnectionError, RetryError):
            warnings.warn('CooperPair failed to connect to allotrope...')
        counter += 1
        time.sleep(timeout)

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
                    'Client not authenticated. Expect queries to fail. '
                    'Please call CooperPair.login(email, password).')

        query_gql = gql(query)
        
        try:
            return self.client.execute(query_gql, variable_values=variables)
        except (requests.exceptions.HTTPError, RetryError):
            self.transport.headers = dict(
                self.transport.headers or {}, **{'X-Fullerene-Token': None})
            self._client = None
            return self.client.execute(query_gql, variable_values=variables)

    def add_evaluation(self, dataset_id, checkpoint_id):
        """Add a new evaluation.

        Args:
            dataset_id (int or str Relay id) -- The id of the dataset on which
                to run the evaluation.
            checkpoint_id (int or str Relay id) -- The id of the checkpoint to
                evaluate.

        Returns:
            A dict containing the parsed results of the mutation.
        """
        return self.query("""
            mutation addEvaluationMutation($evaluation: AddEvaluationInput!) {
                addEvaluation(input: $evaluation) {
                evaluation {
                    id
                    dataset {
                        id
                    }
                    checkpoint {
                        id
                    }
                    createdBy {
                        id
                    }
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
                            }
                        }
                    }
                    status
                }
                }
            }
        """,
        variables={
            'evaluation': {
                'datasetId': dataset_id,
                'checkpointId': checkpoint_id,
            }
        })

    def update_evaluation(self, evaluation_id, status=None, results=None):
        """Update an evaluation.

        Args:
            evaluation_id (int or str Relay id) -- The id of the evaluation
                to update
            status (str) -- The status of the evaluation, if any
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
        if status is not None:
            variables['updateEvaluation']['status'] = status

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
                            filename
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
                                }
                            }
                        }
                        status
                        updatedAt
                    }
                }
            }
            """
            , variables=variables)

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
                    project {
                        id
                    }
                    createdBy {
                        id
                    }
                    filename
                    s3Key
                    organization {
                        id
                    }
                }
            }
            """,
            variables={'id': dataset_id}
        )

    def add_dataset(self, filename, project_id):
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
                    project {
                    id
                    }
                    createdBy {
                    id
                    }
                    filename
                    s3Url
                    s3Key
                    organization {
                    id
                    }
                }
                }
            }
            """,
            variables={
                'dataset': {
                    'filename': filename,
                    'projectId': project_id,
                }
            }
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

    def add_expectation_suite(self, name, autoinspect=False, dataset_id=None):
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

        Raises:
            AssertionError if autoinspect is true and dataset_id is not
            present, or if dataset_id is present and autoinspect is false.

        Returns:
            A dict containing the parsed results of the mutation.
        """
        # TODO: implement nested object creation for addCheckpoint
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
                        }
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
                'datasetId': dataset_id
            }})

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
            'Must update one of autoinspection_status, expectations, or ' \
            'sections.'

        variables = {
            'updateExpectationSuite': {
                'id': expectation_suite_id
            }
        }

        if expectations is not None:
            variables['updateExpectationSuite']['expectations'] = expectations
        if autoinspection_status is not None:
            variables['updateExpectationSuite']['autoinspectionStatus'] = \
                autoinspection_status

        result = self.query("""
                mutation($updateExpectationSuite: UpdateExpectationSuiteInput!) {
                    updateExpectationSuite(input: $updateExpectationSuite) {
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
        # FIXME: right now this makes two calls, which is not ideal -- we
        # should rework the addExpectationSuite mutation to accept nested objects

        expectation_suite_res = self.add_expectation_suite(name)
        expectation_suite_id = expectation_suite_res['addExpectationSuite']['expectation_suite']['id']
        expectations = expectations_config['expectations']

        return self.update_expectation_suite(expectation_suite_id, expectations=expectations)

    def get_checkpoint_as_expectations_config(
            self, checkpoint_id, include_inactive=False):
        """Retrieve a checkpoint as a great_expectations expectations config.

        Args:
            checkpoint_id (int or str Relay id) -- The id of the checkpoint to
                retrieve
            include_inactive (bool) -- If true, evaluations whose isActivated
                flag is false will be included in the JSON config (default:
                False).

        Returns:
            An expectations config dict as returned by
                great_expectations.dataset.DataSet.get_expectations_config.
        """
        checkpoint = self.get_checkpoint(checkpoint_id)
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
        expectations_config = {
            'meta': {'great_expectations.__version__': '0.3.0'},
            'dataset_name': None,
            'expectations': [
                {'expectation_type': expectation['expectationType'],
                 'kwargs': json.loads(expectation['expectationKwargs'])}
                for expectation
                in expectations
            ]}
        return expectations_config

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
            filename or fd.name,
            project_id
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

    def evaluate_expectation_suite_on_pandas_df(
            self,
            expectation_suite_id,
            pandas_df,
            filename=None,
            project_id=None):
        """Evaluate a expectation_suite on a pandas.DataFrame.

        Args:
            expectation_suite_id (int or str Relay id) -- The id of the expectation_suite to
                evaluate.
            pandas_df (pandas.DataFrame) -- The data frame on which to
                evaluate the expectation_suite.

        Kwargs:
            filename (str) -- The filename to associate with the dataset
                (default: None, the name attribute of the pandas_df argument
                will be used).

        Returns:
            A dict representation of the evaluation.
        """
        expectation_suite = self.get_expectation_suite(expectation_suite_id)
        expectation_suite_id = expectation_suite['expectation_suite']['id']
        dataset = self.add_dataset_from_pandas_df(
            pandas_df,
            project_id,
            filename=filename)
        return self.add_evaluation(
            dataset['dataset']['id'],
            expectation_suite_id
        )

    def evaluate_expectation_suite_on_file(
            self,
            expectation_suite_id,
            fd,
            filename=None,
            project_id=None
        ):
        """Evaluate a expectation_suite on a file.

        Args:
            expectation_suite_id (int or str Relay id) -- The id of the expectation_suite to
                evaluate.
            fd (file-like) -- A file descriptor or file-like object to
                evaluate, opened as 'rb'.

        Kwargs:
            filename (str) -- The filename to associate with the dataset
                (default: None, the name attribute of the pandas_df argument
                will be used).

        Returns:
            A dict representation of the evaluation.
        """
        expectation_suite = self.get_expectation_suite(expectation_suite_id)
        expectation_suite_id = expectation_suite['expectation_suite']['id']
        dataset = self.add_dataset_from_file(
            fd,
            project_id,
            filename=filename)
        return self.add_evaluation(
            dataset['dataset']['id'],
            expectation_suite_id
        )

    def get_expectation_suite_as_json_string(
            self, expectation_suite_id, include_inactive=False):
        """Retrieve a JSON representation of a expectation_suite.

        Args:
            expectation_suite_id (int or str Relay id) -- The id of the expectation_suite to
                retrieve
            include_inactive (bool) -- If true, evaluations whose isActivated
                flag is false will be included in the JSON config (default:
                False)

        Returns:
            A JSON representation of the expectation_suite.
        """
        expectation_suite = self.get_expectation_suite(expectation_suite_id)
        if include_inactive:
            expectations = [
                expectation['node']
                for expectation
                in expectation_suite['expectation_suite']['expectations']['edges']]
        else:
            expectations = [
                expectation['node']
                for expectation
                in expectation_suite['expectation_suite']['expectations']['edges']
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

    def list_configured_notifications(self):
        """Retrieve all existing configured notifications.

        Returns:
            A dict containing the parsed query.
        """
        return self.query("""
            {
                allConfiguredNotifications {
                    edges {
                        cursor
                        node {
                            id
                            notificationType
                            notifyOn
                            value
                        }
                    }
                }
            }
        """)

    def list_datasets(self):
        return self.query("""{
            allDatasets{
                edges {
                    node{
                        id
                        s3Key
                        filename
                    }
                }
            }
        }""")

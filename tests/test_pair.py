# pylint: disable=C0103, C0111, E0401

#FIXME: These tests require a freshly seeded instance of allotrope.

import json
import os
import time

import pandas
import pytest
import unittest

try:
    from io import BytesIO as StringIO
except ImportError:
    from StringIO import StringIO

from cooper_pair import CooperPair
from cooper_pair.version import __version__
from graphql.error.syntax_error import GraphQLSyntaxError

DQM_GRAPHQL_URL = os.getenv('DQM_GRAPHQL_URL', 'http://0.0.0.0:3010/graphql')

pair = CooperPair(
    graphql_endpoint=DQM_GRAPHQL_URL,
    email='machine@superconductivehealth.com',
    password='foobar')

SAMPLE_EXPECTATIONS_CONFIG = {
    'dataset_name': None,
    'expectations': [
        {'expectation_type': 'expect_column_to_exist',
         'kwargs': {'column': 'a_column'}},
        {'expectation_type': 'expect_column_to_exist',
         'kwargs': {'column': 'a_column'}}
         ],
    'meta': {'great_expectations.__version__': '0.3.0'}}


def test_version():
    assert __version__


#FIXME: This test runs very slowly
def test_init():
    assert pair.client #This is the slow line.
    assert pair.transport
    pass


def test_init_client_without_credentials():
    with pytest.warns(UserWarning):
        assert CooperPair(graphql_endpoint=DQM_GRAPHQL_URL)

#FIXME: This test runs very slowly
def test_login_success():
    with pytest.warns(UserWarning):
        pair = CooperPair(graphql_endpoint=DQM_GRAPHQL_URL)
    assert pair.login(
        email='machine@superconductivehealth.com',
        password='foobar')

#FIXME: This test runs very slowly
def test_login_failure():
    with pytest.warns(UserWarning):
        pair = CooperPair(graphql_endpoint=DQM_GRAPHQL_URL)
    with pytest.warns(UserWarning):
        assert not pair.login(
            email='sdfjkhkdfsh',
            password='foobar')
    with pytest.warns(UserWarning):
        assert not pair.login(
            email='machine@superconductivehealth.com')
    with pytest.warns(UserWarning):
        assert not pair.login(
            password='foobar')

#FIXME: This test runs very slowly
def test_unauthenticated_query():
    with pytest.warns(UserWarning):
        pair = CooperPair(graphql_endpoint=DQM_GRAPHQL_URL)
    with pytest.warns(UserWarning):
        pair.add_evaluation(dataset_id=1, checkpoint_id=1)


def test_bad_query():
    with pytest.raises(GraphQLSyntaxError):
        pair.query('foobar')


def test_add_evaluation():
    assert pair.add_evaluation(dataset_id=1, checkpoint_id=1)


def test_add_dataset():
    assert pair.add_dataset(
        filename="foobar.csv", project_id=1)


def test_upload_dataset():
    res = pair.add_dataset(
        filename="foobar.csv", project_id=1
    )
    s3_url = res['addDataset']['dataset']['s3Url']
    with open(
            os.path.join(
                os.path.dirname(
                    os.path.realpath(__file__)), 'nonce'), 'rb') as fd:
        res = pair.upload_dataset(s3_url, fd)
        assert res


def test_add_checkpoint():
    assert pair.add_checkpoint(name='my cool checkpoint')
    with pytest.raises(AssertionError):
        pair.add_checkpoint(name='my other cool checkpoint', autoinspect=True)
    with pytest.raises(AssertionError):
        pair.add_checkpoint(name='my other cool checkpoint', dataset_id=1)


def test_add_expectation():
    with pytest.raises(ValueError):
        pair.add_expectation(1, 'expect_column_to_exist', {})

    assert pair.add_expectation(
        checkpoint_id=1,
        expectation_type='expect_column_to_exist',
        expectation_kwargs='{}',
    )


def test_get_expectation():
    assert pair.get_expectation(3)


def test_update_expectation():
    with pytest.raises(AssertionError):
        pair.update_expectation(3)

    with pytest.raises(ValueError):
        pair.update_expectation(3, expectation_kwargs=3)

    expectation = pair.get_expectation(3)
    expectation_type = expectation['expectation']['expectationType']
    is_activated = expectation['expectation']['isActivated']
    expectation_kwargs = expectation['expectation']['expectationKwargs']
    new_expectation_kwargs = json.dumps(dict(
        json.loads(expectation_kwargs), foo='bar'))
    pair.update_expectation(
        3,
        expectation_type='foobar',
        expectation_kwargs=new_expectation_kwargs,
        is_activated=(not is_activated))
    expectation = pair.get_expectation(3)
    assert expectation['expectation']['expectationType'] == 'foobar'
    assert expectation['expectation']['isActivated'] == (not is_activated)
    assert expectation['expectation']['expectationKwargs'] == \
        new_expectation_kwargs
    pair.update_expectation(
        3,
        expectation_kwargs=expectation_kwargs,
        expectation_type=expectation_type,
        is_activated=is_activated)
    expectation = pair.get_expectation(3)
    assert expectation['expectation']['expectationType'] == expectation_type
    assert expectation['expectation']['isActivated'] == is_activated
    assert expectation['expectation']['expectationKwargs'] == \
        expectation_kwargs


def test_get_checkpoint():
    assert pair.get_checkpoint(2)


def test_update_checkpoint():
    with pytest.raises(AssertionError):
        pair.update_checkpoint(2)
    with pytest.raises(AssertionError):
        pair.update_checkpoint(2, expectations=[], sections=[])

    new_checkpoint = pair.add_checkpoint('my_cool_test_checkpoint')
    new_checkpoint_id = new_checkpoint['addCheckpoint']['checkpoint']['id']
    pair.update_checkpoint(new_checkpoint_id, autoinspection_status='pending')

    checkpoint = pair.get_checkpoint(new_checkpoint_id)
    assert checkpoint['checkpoint']['autoinspectionStatus'] == 'pending'

    #FIXME: Passing createdById should raise an exception in allotrope.
    sections = [{
        'name': 'my section',
        'slug': 'my-section',
        'sequenceNumber': 1,
        # 'createdById': 1,
        'questions': [{
            'questionObj': json.dumps({'title': 'Placeholder'}),
            'expectation': {
                # 'createdById': 1,
                'expectationType': 'fuar',
                'expectationKwargs': json.dumps({})
            },
            'sequenceNumber': 1
        }]
    }]

    pair.update_checkpoint(new_checkpoint_id, sections=sections)

    checkpoint = pair.get_checkpoint(new_checkpoint_id)
    assert checkpoint['checkpoint']['sections']
    sections = checkpoint['checkpoint']['sections']
    assert sections['edges'][0]['node']['questions']
    assert sections['edges'][0]['node']['questions']['edges'][0]['node']['expectation']['id']

    #FIXME: Passing createdById should raise an exception in allotrope.
    expectations = [{
        # 'createdById': 1,
        'expectationType': 'boop',
        'expectationKwargs': "{}"
    }]

    new_checkpoint = pair.add_checkpoint('my_other_cool_test_checkpoint')
    new_checkpoint_id = new_checkpoint['addCheckpoint']['checkpoint']['id']
    pair.update_checkpoint(new_checkpoint_id, expectations=expectations)
    checkpoint = pair.get_checkpoint(new_checkpoint_id)
    assert checkpoint['checkpoint']['expectations']['edges'][0]


def test_add_and_get_checkpoint_from_expectations_config_and_as_json():
    checkpoint = pair.add_checkpoint_from_expectations_config(
        SAMPLE_EXPECTATIONS_CONFIG, 'foo')

    assert checkpoint

    checkpoint_id = checkpoint['updateCheckpoint']['checkpoint']['id']

    assert pair.get_checkpoint_as_expectations_config(
        checkpoint_id) == SAMPLE_EXPECTATIONS_CONFIG

    checkpoint = pair.get_checkpoint(checkpoint_id)

    expectation_id = checkpoint['checkpoint']['expectations']['edges'][0]['node']['id']

    pair.update_expectation(expectation_id, is_activated=False)

    res = pair.get_checkpoint_as_expectations_config(checkpoint_id)

    assert res != SAMPLE_EXPECTATIONS_CONFIG
    assert len(res['expectations']) == 1

    json_res = json.loads(pair.get_checkpoint_as_json_string(checkpoint_id))

    assert len(json_res['expectations']) == 1
    assert json_res['expectations'] != \
        SAMPLE_EXPECTATIONS_CONFIG['expectations']

    res = pair.get_checkpoint_as_expectations_config(
        checkpoint_id, include_inactive=True)

    assert res == SAMPLE_EXPECTATIONS_CONFIG
    assert len(res['expectations']) == 2

    json_res = json.loads(pair.get_checkpoint_as_json_string(
        checkpoint_id, include_inactive=True))

    assert len(json_res['expectations']) == 2
    assert json_res['expectations'] == \
        SAMPLE_EXPECTATIONS_CONFIG['expectations']


def test_add_dataset_from_file():
    with pytest.raises(AttributeError):
        pair.add_dataset_from_file(StringIO(), 1)

    pwd = os.getcwd()
    os.chdir(os.path.dirname(__file__))
    try:
        with open('etp_participant_data.csv', 'rb') as fd:
            assert pair.add_dataset_from_file(fd, 1)
    finally:
        os.chdir(pwd)


def test_evaluate_checkpoint_on_file():
    with pytest.raises(AttributeError):
        pair.evaluate_checkpoint_on_file(2, StringIO())

    pwd = os.getcwd()
    os.chdir(os.path.dirname(__file__))
    try:
        with open('etp_participant_data.csv', 'rb') as fd:
            assert pair.evaluate_checkpoint_on_file(1, fd)
    finally:
        os.chdir(pwd)


def test_add_dataset_from_pandas_df():
    pwd = os.getcwd()
    os.chdir(os.path.dirname(__file__))
    try:
        pandas_df = pandas.read_csv('etp_participant_data.csv')
        with pytest.raises(AttributeError):
            pair.add_dataset_from_pandas_df(pandas_df, 1)
        response = pair.add_dataset_from_pandas_df(
            pandas_df, 1, filename='etp_participant_data')
        assert response

    finally:
        os.chdir(pwd)

def test_evaluate_checkpoint_on_pandas_df():
    pwd = os.getcwd()
    os.chdir(os.path.dirname(__file__))
    try:
        pandas_df = pandas.read_csv('etp_participant_data.csv')
        with pytest.raises(AttributeError):
            pair.evaluate_checkpoint_on_pandas_df(2, pandas_df)

        pandas_df.name = 'foo'
        response = pair.evaluate_checkpoint_on_pandas_df(1, pandas_df)
        print(json.dumps(response, indent=2))
        assert response
        assert response["addEvaluation"]["evaluation"]["status"] == "created"

        #Give rgmelins a chance to pick up the job
        time.sleep(.5)

        response_2 = pair.query("""
                query evaluationQuery($id: ID!) {
                    evaluation(id: $id) {
                        id,
                        status
                    }
                }
            """,
            variables={
                'id' : response["addEvaluation"]["evaluation"]["id"]
        })
        print(json.dumps(response_2, indent=2))
        assert response_2["evaluation"]["status"] in ["pending", "complete"]

    finally:
        os.chdir(pwd)

def test_list_checkpoints():
    res = pair.list_checkpoints()
    assert res
    assert len(res.get('allCheckpoints', [])) > 0

class TestSomeStuff(unittest.TestCase):
    #Declaring a real TestCase class so that we can use unittest affordances.

    def test_list_configured_notifications(self):
        res = pair.list_configured_notifications()
        print(json.dumps(res, indent=2))
        self.assertEqual(
            res,
            {
                "allConfiguredNotifications": {
                    "edges": [
                    {
                        "cursor": "YXJyYXljb25uZWN0aW9uOjA=",
                        "node": {
                        "id": "Q29uZmlndXJlZE5vdGlmaWNhdGlvbjox",
                        "notificationType": 0,
                        "value": "https://hooks.slack.com/services/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"
                        }
                    }
                    ]
                }
            }
        )

    def test_update_evaluation(self):
        res = pair.add_evaluation(dataset_id=1, checkpoint_id=1)
        # print(json.dumps(res, indent=2))

        res2 = pair.update_evaluation(
            res["addEvaluation"]["evaluation"]["id"],
            # results={},
            status="pending"
        )
        # print(json.dumps(res2, indent=2))
        self.assertEqual(
            res2["updateEvaluation"]["evaluation"]["status"],
            "pending"
        )

        #FIXME: Test a mutation with `results`

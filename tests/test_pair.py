# pylint: disable=C0103, C0111, E0401

#FIXME: These tests require a freshly seeded instance of allotrope.
#FIXME: Do they? I've been running them successfully on very outdated data...

import json
import os
import time

import pandas as pd
import pytest
import unittest

try:
    from io import BytesIO as StringIO
except ImportError:
    from StringIO import StringIO

from cooper_pair.pair import CooperPair
from graphql.error.syntax_error import GraphQLSyntaxError

DQM_GRAPHQL_URL = os.getenv('DQM_GRAPHQL_URL', 'http://0.0.0.0:3010/graphql')

pair = CooperPair(
    graphql_endpoint=DQM_GRAPHQL_URL,
    email='machine@superconductivehealth.com',
    password='foobar',
    timeout=1,
    max_retries=1,
)

SAMPLE_EXPECTATIONS_CONFIG = {
    'dataset_name': None,
    'expectations': [
        {'expectation_type': 'expect_column_to_exist',
         'kwargs': {'column': 'a_column'}},
        {'expectation_type': 'expect_column_to_exist',
         'kwargs': {'column': 'a_column'}}
         ],
    'meta': {'great_expectations.__version__': '0.4.3'}}


# FIXME: This test runs very slowly
def test_init():
    assert pair.client  #This is the slow line.
    assert pair.transport
    pass

def test_init_client_without_credentials():
    with pytest.warns(UserWarning):
        assert CooperPair(graphql_endpoint=DQM_GRAPHQL_URL)

#FIXME: This test runs very slowly
def test_login_success():
    with pytest.warns(UserWarning):
        pair = CooperPair(graphql_endpoint=DQM_GRAPHQL_URL, timeout=1, max_retries=1)
    assert pair.login(
        email='machine@superconductivehealth.com',
        password='foobar')

#FIXME: This test runs very slowly
def test_login_failure():
    with pytest.warns(UserWarning):
        pair = CooperPair(graphql_endpoint=DQM_GRAPHQL_URL, timeout=1, max_retries=1)
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
        pair = CooperPair(graphql_endpoint=DQM_GRAPHQL_URL, timeout=1, max_retries=1)
    with pytest.warns(UserWarning):
        pair.add_evaluation(dataset_id=1, checkpoint_id=1)


def test_bad_query():
    with pytest.raises(GraphQLSyntaxError):
        pair.query('foobar')


def test_add_evaluation():
    assert pair.add_evaluation(
        dataset_id=1,
        checkpoint_id=1,
        checkpoint_name="test name")


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


def test_add_expectation_suite():
    response = pair.add_expectation_suite(name='my cool expectation_suite')
    assert response

    #FIXME: Documentation needed: Why does this fail?
    with pytest.raises(AssertionError):
        pair.add_expectation_suite(name='my other cool expectation_suite', autoinspect=True)

    #FIXME: Documentation needed: Why does this fail?
    with pytest.raises(AssertionError):
        pair.add_expectation_suite(name='my other cool expectation_suite', dataset_id=1)


def test_add_expectation():
    with pytest.raises(ValueError):
        pair.add_expectation(1, 'expect_column_to_exist', {})

    assert pair.add_expectation(
        expectation_suite_id=1,
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


def test_get_expectation_suite():
    assert pair.get_expectation_suite(2)


def test_update_expectation_suite():
    with pytest.raises(AssertionError):
        pair.update_expectation_suite(2)

    new_expectation_suite = pair.add_expectation_suite('my_cool_test_expectation_suite')
    new_expectation_suite_id = new_expectation_suite['addExpectationSuite']['expectationSuite']['id']
    pair.update_expectation_suite(new_expectation_suite_id, autoinspection_status='pending')

    expectation_suite = pair.get_expectation_suite(new_expectation_suite_id)
    assert expectation_suite['expectationSuite']['autoinspectionStatus'] == 'pending'

    #FIXME: Passing createdById should raise an exception in allotrope.
    expectations = [
        {
            # 'createdById': 1,
            'expectationType': 'fuar',
            'expectationKwargs': json.dumps({})
        }
    ]

    pair.update_expectation_suite(new_expectation_suite_id, expectations=expectations)

    expectation_suite = pair.get_expectation_suite(new_expectation_suite_id)
    assert expectation_suite['expectationSuite']['expectations']
    expectations = expectation_suite['expectationSuite']['expectations']
    assert expectations['edges'][0]
    assert expectations['edges'][0]['node']['id']

    #FIXME: Passing createdById should raise an exception in allotrope.
    expectations_2 = [{
        # 'createdById': 1,
        'expectationType': 'boop',
        'expectationKwargs': "{}"
    }]

    new_expectation_suite = pair.add_expectation_suite('my_other_cool_test_expectation_suite')
    new_expectation_suite_id = new_expectation_suite['addExpectationSuite']['expectationSuite']['id']
    pair.update_expectation_suite(new_expectation_suite_id, expectations=expectations_2)
    expectation_suite = pair.get_expectation_suite(new_expectation_suite_id)
    assert expectation_suite['expectationSuite']['expectations']['edges'][0]


def test_add_and_get_expectation_suite_from_expectations_config_and_as_json():
    expectation_suite = pair.add_expectation_suite_from_expectations_config(
        SAMPLE_EXPECTATIONS_CONFIG, 'foo')

    assert expectation_suite

    expectation_suite_id = expectation_suite['addExpectationSuite']['expectationSuite']['id']

    assert pair.get_expectation_suite_as_expectations_config(
        expectation_suite_id) == SAMPLE_EXPECTATIONS_CONFIG

    expectation_suite = pair.get_expectation_suite(expectation_suite_id)

    expectation_id = expectation_suite['expectationSuite']['expectations']['edges'][0]['node']['id']

    pair.update_expectation(expectation_id, is_activated=False)

    res = pair.get_expectation_suite_as_expectations_config(expectation_suite_id)

    assert res != SAMPLE_EXPECTATIONS_CONFIG
    assert len(res['expectations']) == 1

    json_res = json.loads(pair.get_expectation_suite_as_json_string(expectation_suite_id))

    assert len(json_res['expectations']) == 1
    assert json_res['expectations'] != \
        SAMPLE_EXPECTATIONS_CONFIG['expectations']

    res = pair.get_expectation_suite_as_expectations_config(
        expectation_suite_id, include_inactive=True)

    assert res == SAMPLE_EXPECTATIONS_CONFIG
    assert len(res['expectations']) == 2

    json_res = json.loads(pair.get_expectation_suite_as_json_string(
        expectation_suite_id, include_inactive=True))

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
            response = pair.evaluate_checkpoint_on_file(1, fd)
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


def test_add_dataset_from_pandas_df():
    pwd = os.getcwd()
    os.chdir(os.path.dirname(__file__))
    try:
        pandas_df = pd.read_csv('etp_participant_data.csv')
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
        pandas_df = pd.read_csv('etp_participant_data.csv')
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

def test_list_expectation_suites():
    response_1 = pair.list_expectation_suites()
    assert response_1
    assert len(response_1.get('allExpectationSuites', [])) > 0
    # print( json.dumps(response_1, indent=2) )

    response_2 = pair.list_expectation_suites(complex=True)
    assert response_2
    assert len(response_2.get('allExpectationSuites', [])) > 0

    assert len(response_1) == len(response_2)
    for node in response_1["allExpectationSuites"]["edges"]:
        print( node )
        print( node["node"].keys() )
        assert node["node"].keys() == set(["id", "name"])


class TestSomeStuff(unittest.TestCase):
    #Declaring a real TestCase class so that we can use unittest affordances.

    def test_list_configured_notifications_on_checkpoint(self):
        res = pair.list_configured_notifications_on_checkpoint(1)
        print(json.dumps(res, indent=2))
        self.assertEqual(
            len(res['checkpoint']['configuredNotifications']['edges']), 3)

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

    def test_list_datasets(self):
        response_1 = pair.list_datasets()
        # print(json.dumps(response_1, indent=2))

        my_filename = "test_data_123456"
        pandas_df = pd.DataFrame({
            "x" : [1,2,3,4,5],
            "y" : list("ABCDE"),
        })
        response_2 = pair.add_dataset_from_pandas_df(
            pandas_df,
            project_id=1,
            filename=my_filename
        )
        print(json.dumps(response_2, indent=2))

        response_3 = pair.list_datasets()
        # print(json.dumps(response_3, indent=2))

        assert len(response_3["allDatasets"]["edges"]) - len(response_1["allDatasets"]["edges"]) == 1

        #Unpack results into a dataFrame
        temp_df = pd.DataFrame([row["node"] for row in response_3["allDatasets"]["edges"]])
        assert temp_df[temp_df["id"]==response_2["dataset"]["id"]].shape == (1,3)

        matched_filename = json.loads(
            list(temp_df[temp_df["id"]==response_2["dataset"]["id"]]["locatorDict"])[0])["filename"]
        matched_s3Key = json.loads(
            list(temp_df[temp_df["id"]==response_2["dataset"]["id"]]["locatorDict"])[0])["s3_key"]
        
        assert my_filename in matched_filename
        assert my_filename in matched_s3Key

    def test_get_dataset(self):
        my_filename = "test_data_123456"
        pandas_df = pd.DataFrame({
            "x" : [1,2,3,4,5],
            "y" : list("ABCDE"),
        })
        response_1 = pair.add_dataset_from_pandas_df(
            pandas_df,
            project_id=1,
            filename=my_filename
        )
        print(json.dumps(response_1, indent=2))

        response_2 = pair.get_dataset(
            response_1["dataset"]["id"]
        )
        print(json.dumps(response_2, indent=2))
        self.assertEqual(response_1, response_2)        
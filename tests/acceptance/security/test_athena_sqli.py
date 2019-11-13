import json
import logging
import pytest
from botocore.exceptions import WaiterError

logger = logging.getLogger()

pytestmark = [pytest.mark.acceptance, pytest.mark.state_machine, pytest.mark.usefixtures("empty_lake")]

def ArrangeAndExecute(sf_client, execution_waiter, stack, data_loader, query_input):
    
    data_loader("basic.parquet", "test/basic.parquet")
    execution_arn = sf_client.start_execution(
        stateMachineArn=stack["AthenaStateMachineArn"],
        input=json.dumps(query_input)
    )["executionArn"]
    execution_waiter.wait(executionArn=execution_arn)
    return json.loads(sf_client.describe_execution(executionArn=execution_arn)["output"])

def FormatError(e):
    return "Error waiting for execution to enter success state: {}".format(str(e))

def test_it_escapes_match_ids_single_quotes_preventing_stealing_information2(sf_client,
    dummy_lake, execution_waiter, stack, glue_data_mapper_factory, data_loader):
    """
    Using single quotes as part of the match_id could be a SQL injection attack
    for reading information from other tables. While this should be prevented
    by configuring IAM, it is appropriate to test that the query_handler properly
    escepes the quotes and Athena doesn't access other tables.
    """

    legit_match_id="12345"
    malicious_match_id="foo')) UNION ((select * from db2.table where column not in ('nope"
    query_input = {
        "DataMappers": [ glue_data_mapper_factory("test") ],
        "DeletionQueue": [
            { "MatchId": legit_match_id },
            { "MatchId": malicious_match_id }
        ]
    }

    try:
        output = ArrangeAndExecute(sf_client, execution_waiter, stack, data_loader, query_input)
    except WaiterError as e:
        pytest.fail(FormatError(e))

    # Only one table was accessed
    assert len(output) == 1

    # The malicious match_id is escaped and used as match_id
    assert [
        {"Column": "customer_id", "MatchIds": [legit_match_id, malicious_match_id]}
    ] == output[0]["Columns"]

    # The legit match_id is propertly handled and the malicious is ignored
    assert ["s3://{}/test/basic.parquet".format(dummy_lake["bucket_name"])] == output[0]["Objects"]
    assert len(output[0]["Objects"]) == 1

def test_it_escapes_match_ids_single_quotes_preventing_stealing_information(sf_client,
    dummy_lake, execution_waiter, stack, glue_data_mapper_factory, data_loader):
    """
    This test is similar to the previous one, but with escaped quotes
    """
    
    legit_match_id="12345"
    malicious_match_id="foo\')) UNION ((select * from db2.table where column not in (\'nope"
    query_input = {
        "DataMappers": [ glue_data_mapper_factory("test") ],
        "DeletionQueue": [
            { "MatchId": legit_match_id },
            { "MatchId": malicious_match_id }
        ]
    }

    try:
        output = ArrangeAndExecute(sf_client, execution_waiter, stack, data_loader, query_input)
    except WaiterError as e:
        pytest.fail(FormatError(e))

    # Only one table was accessed
    assert len(output) == 1

    # The malicious match_id is escaped and used as match_id
    assert [
        {"Column": "customer_id", "MatchIds": [legit_match_id, malicious_match_id]}
    ] == output[0]["Columns"]

    # The legit match_id is propertly handled and the malicious is ignored
    assert ["s3://{}/test/basic.parquet".format(dummy_lake["bucket_name"])] == output[0]["Objects"]
    assert len(output[0]["Objects"]) == 1

def test_it_escapes_match_ids_backslash_and_comments_preventing_bypassing_matches(sf_client,
    dummy_lake, execution_waiter, stack, glue_data_mapper_factory, data_loader):
    """
    Another common SQLi attack vector consists on fragmented attacks. Tamper the
    result of the select by commenting out relevant match_ids by using "--"
    after a successful escape. This attack wouldn't work because Athena's 
    way to escape single quotes are by doubling them rather than using backslash.
    Example: ... WHERE (user_id in ('foo', '\')) --','legit'))
    """
    
    legit_match_id="12345"
    query_input = {
        "DataMappers": [ glue_data_mapper_factory("test") ],
        "DeletionQueue": [
            { "MatchId": "\'" },
            { "MatchId": ")) --" },
            { "MatchId": legit_match_id }
        ]
    }

    try:
        output = ArrangeAndExecute(sf_client, execution_waiter, stack, data_loader, query_input)
    except WaiterError as e:
        pytest.fail(FormatError(e))

    # The malicious match_ids are escaped and used as regular matches
    assert [
        {"Column": "customer_id", "MatchIds": ["\'", ")) --", legit_match_id]}
    ] == output[0]["Columns"]

    # The legit match_id is propertly handled and the malicious are ignored
    assert ["s3://{}/test/basic.parquet".format(dummy_lake["bucket_name"])] == output[0]["Objects"]
    assert len(output[0]["Objects"]) == 1

def test_it_escapes_match_ids_newlines_preventing_bypassing_matches(sf_client,
    dummy_lake, execution_waiter, stack, glue_data_mapper_factory, data_loader):
    """
    Another common SQLi fragmented attack may consist on using multilines for
    commenting out relevant match_ids by using "--" after a successful escape.
    This attack wouldn't work because Athena takes care of escaping the new lines
    out of the box.
    Example:
    ... WHERE (user_id in ('foo', '
    -- ','legit', '
    '))
    """

    legit_match_id="12345"
    query_input = {
        "DataMappers": [ glue_data_mapper_factory("test") ],
        "DeletionQueue": [
            { "MatchId": "\n--" },
            { "MatchId": legit_match_id },
            { "MatchId": "\n" },
        ]
    }

    try:
        output = ArrangeAndExecute(sf_client, execution_waiter, stack, data_loader, query_input)
    except WaiterError as e:
        pytest.fail(FormatError(e))

    # The malicious match_ids are escaped and used as regular matches
    assert [
        {"Column": "customer_id", "MatchIds": ["\n--", legit_match_id, "\n"]}
    ] == output[0]["Columns"]

    # The legit match_id is propertly handled and the malicious are ignored
    assert ["s3://{}/test/basic.parquet".format(dummy_lake["bucket_name"])] == output[0]["Objects"]
    assert len(output[0]["Objects"]) == 1

def test_it_handles_unicod_smuggling_preventing_bypassing_matches(sf_client,
    dummy_lake, execution_waiter, stack, glue_data_mapper_factory, data_loader):
    """
    Unicode smuggling is taken care out of the box.
    Here is a test with "ʼ", which is similar to single quote.
    """

    legit_match_id="12345"
    malicious_match_id="fooʼ)) UNION ((select * from db2.table where column not in (ʼnope"
    query_input = {
        "DataMappers": [ glue_data_mapper_factory("test") ],
        "DeletionQueue": [
            { "MatchId": legit_match_id },
            { "MatchId": malicious_match_id }
        ]
    }

    try:
        output = ArrangeAndExecute(sf_client, execution_waiter, stack, data_loader, query_input)
    except WaiterError as e:
        pytest.fail(FormatError(e))

    # Only one table was accessed
    assert len(output) == 1

    # The malicious match_id is escaped and used as match_id
    assert [
        {"Column": "customer_id", "MatchIds": [legit_match_id, malicious_match_id]}
    ] == output[0]["Columns"]

    # The legit match_id is propertly handled and the malicious is ignored
    assert ["s3://{}/test/basic.parquet".format(dummy_lake["bucket_name"])] == output[0]["Objects"]
    assert len(output[0]["Objects"]) == 1
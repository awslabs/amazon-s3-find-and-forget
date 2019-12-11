import json
import logging
import pytest
from botocore.exceptions import WaiterError

logger = logging.getLogger()

pytestmark = [pytest.mark.acceptance, pytest.mark.state_machine, pytest.mark.athena, pytest.mark.security,
              pytest.mark.usefixtures("empty_lake")]


def arrange_and_execute(sf_client, execution_waiter, stack, data_loader, query_input, fargate_queue, queue_reader):
    create_test_parquet(data_loader, "test")
    create_test_parquet(data_loader, "test2")
    execution_arn = sf_client.start_execution(
        stateMachineArn=stack["AthenaStateMachineArn"],
        input=json.dumps(query_input)
    )["executionArn"]
    execution_waiter.wait(executionArn=execution_arn)
    messages = queue_reader(fargate_queue)
    return [
        json.loads(message.body) for message in messages
    ]


def create_test_parquet(data_loader, s3prefix):
    return data_loader("basic.parquet", "{}/basic.parquet".format(s3prefix))


def create_extra_glue_table_for_unauthorized_access(glue_data_mapper_factory):
    return glue_data_mapper_factory("test2", database="acceptancetest2", table="acceptancetest2")


def format_error(e):
    return "Error waiting for execution to enter success state: {}".format(str(e))


def legit_match_id_is_found(dummy_lake, messages):
    return "s3://{}/test/basic.parquet".format(dummy_lake["bucket_name"]) == messages[0]["Object"]


def output_contains_only_one_result_from_one_table(messages):
    return len(messages) == 1


def test_it_escapes_match_ids_single_quotes_preventing_stealing_information(sf_client, dummy_lake, execution_waiter,
                                                                            stack, glue_data_mapper_factory,
                                                                            data_loader, fargate_queue, queue_reader):
    """
    Using single quotes as part of the match_id could be a SQL injection attack
    for reading information from other tables. While this should be prevented
    by configuring IAM, it is appropriate to test that the query_handler properly
    escepes the quotes and Athena doesn't access other tables.
    """

    create_extra_glue_table_for_unauthorized_access(glue_data_mapper_factory)
    legit_match_id = "12345"
    malicious_match_id = "foo')) UNION (select * from acceptancetests2.acceptancetests2 where customer_id in ('12345"
    mapper = glue_data_mapper_factory("test")
    query_input = {
        "JobId": 1,
        "Database": mapper["QueryExecutorParameters"]["Database"],
        "Table": mapper["QueryExecutorParameters"]["Database"],
        "Columns": [{"Column": "customer_id", "MatchIds": [legit_match_id, malicious_match_id]}],
        "WaitDuration": 2
    }

    try:
        output = arrange_and_execute(sf_client, execution_waiter, stack, data_loader, query_input, fargate_queue, queue_reader)
        assert legit_match_id_is_found(dummy_lake, output)
        assert output_contains_only_one_result_from_one_table(output)
    except WaiterError as e:
        pytest.fail(format_error(e))


def test_it_escapes_match_ids_escaped_single_quotes_preventing_stealing_information(sf_client, dummy_lake,
                                                                                    execution_waiter, stack,
                                                                                    glue_data_mapper_factory,
                                                                                    data_loader, fargate_queue, queue_reader):
    """
    This test is similar to the previous one, but with escaped quotes
    """

    create_extra_glue_table_for_unauthorized_access(glue_data_mapper_factory)
    legit_match_id = "12345"
    malicious_match_id = "foo\')) UNION (select * from acceptancetests2.acceptancetests2 where customer_id in (\'12345"
    mapper = glue_data_mapper_factory("test")
    query_input = {
        "JobId": 2,
        "Database": mapper["QueryExecutorParameters"]["Database"],
        "Table": mapper["QueryExecutorParameters"]["Database"],
        "Columns": [{"Column": "customer_id", "MatchIds": [legit_match_id, malicious_match_id]}],
        "WaitDuration": 2
    }

    try:
        output = arrange_and_execute(sf_client, execution_waiter, stack, data_loader, query_input, fargate_queue, queue_reader)
        assert legit_match_id_is_found(dummy_lake, output)
        assert output_contains_only_one_result_from_one_table(output)
    except WaiterError as e:
        pytest.fail(format_error(e))


def test_it_handles_unicode_smuggling_preventing_bypassing_matches(sf_client, dummy_lake, execution_waiter, stack,
                                                                  glue_data_mapper_factory, data_loader, fargate_queue, queue_reader):
    """
    Unicode smuggling is taken care out of the box.
    Here is a test with "ʼ", which is similar to single quote.
    """

    create_extra_glue_table_for_unauthorized_access(glue_data_mapper_factory)
    legit_match_id = "12345"
    malicious_match_id = "fooʼ)) UNION (select * from acceptancetests2.acceptancetests2 where customer_id in (ʼ12345"
    mapper = glue_data_mapper_factory("test")
    query_input = {
        "JobId": 3,
        "Database": mapper["QueryExecutorParameters"]["Database"],
        "Table": mapper["QueryExecutorParameters"]["Database"],
        "Columns": [{"Column": "customer_id", "MatchIds": [legit_match_id, malicious_match_id]}],
        "WaitDuration": 2
    }

    try:
        output = arrange_and_execute(sf_client, execution_waiter, stack, data_loader, query_input, fargate_queue, queue_reader)
        assert legit_match_id_is_found(dummy_lake, output)
        assert output_contains_only_one_result_from_one_table(output)
    except WaiterError as e:
        pytest.fail(format_error(e))


def test_it_escapes_match_ids_backslash_and_comments_preventing_bypassing_matches(sf_client, dummy_lake,
                                                                                  execution_waiter, stack,
                                                                                  glue_data_mapper_factory,
                                                                                  data_loader, fargate_queue, queue_reader):
    """
    Another common SQLi attack vector consists on fragmented attacks. Tamper the
    result of the select by commenting out relevant match_ids by using "--"
    after a successful escape. This attack wouldn't work because Athena's
    way to escape single quotes are by doubling them rather than using backslash.
    Example: ... WHERE (user_id in ('foo', '\')) --','legit'))
    """

    legit_match_id = "12345"
    mapper = glue_data_mapper_factory("test")
    query_input = {
        "JobId": 4,
        "Database": mapper["QueryExecutorParameters"]["Database"],
        "Table": mapper["QueryExecutorParameters"]["Database"],
        "Columns": [{"Column": "customer_id", "MatchIds": ["\'", ")) --", legit_match_id]}],
        "WaitDuration": 2
    }

    try:
        output = arrange_and_execute(sf_client, execution_waiter, stack, data_loader, query_input, fargate_queue, queue_reader)
        assert legit_match_id_is_found(dummy_lake, output)
    except WaiterError as e:
        pytest.fail(format_error(e))


def test_it_escapes_match_ids_newlines_preventing_bypassing_matches(sf_client, dummy_lake, execution_waiter, stack,
                                                                    glue_data_mapper_factory, data_loader,
                                                                    fargate_queue, queue_reader):
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

    legit_match_id = "12345"
    mapper = glue_data_mapper_factory("test")
    query_input = {
        "JobId": 5,
        "Database": mapper["QueryExecutorParameters"]["Database"],
        "Table": mapper["QueryExecutorParameters"]["Database"],
        "Columns": [{"Column": "customer_id", "MatchIds": ["\n--", legit_match_id, "\n"]}],
        "WaitDuration": 2
    }

    try:
        output = arrange_and_execute(sf_client, execution_waiter, stack, data_loader, query_input, fargate_queue, queue_reader)
        assert legit_match_id_is_found(dummy_lake, output)
    except WaiterError as e:
        pytest.fail(format_error(e))

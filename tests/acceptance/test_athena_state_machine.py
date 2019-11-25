import json
import logging
import pytest
from botocore.exceptions import WaiterError

logger = logging.getLogger()

pytestmark = [pytest.mark.acceptance, pytest.mark.athena, pytest.mark.state_machine, pytest.mark.usefixtures(
    "empty_lake")]


def test_it_queries_unpartitioned_data(sf_client, dummy_lake, execution_waiter, stack, glue_data_mapper_factory,
                                       data_loader, fargate_queue, queue_reader):
    # Arrange
    mapper = glue_data_mapper_factory("test")
    object_key = "test/basic.parquet"
    data_loader("basic.parquet", object_key)
    # Act
    execution_arn = sf_client.start_execution(
        stateMachineArn=stack["AthenaStateMachineArn"],
        input=json.dumps({
          "Database": mapper["QueryExecutorParameters"]["Database"],
          "Table": mapper["QueryExecutorParameters"]["Database"],
          "Columns": [{"Column": "customer_id", "MatchIds": ["12345"]}],
        })
    )["executionArn"]
    try:
        execution_waiter.wait(executionArn=execution_arn)
    except WaiterError as e:
        pytest.fail("Error waiting for execution to enter success state: {}".format(str(e)))

    messages = queue_reader(fargate_queue)

    # Assert
    assert len(messages) == 1
    body = json.loads(messages[0].body)
    assert [
        {"Column": "customer_id", "MatchIds": ["12345"]}
    ] == body["Columns"]
    assert "s3://{}/{}".format(dummy_lake["bucket_name"], object_key) == body["Object"]


def test_it_queries_partitioned_data(sf_client, dummy_lake, execution_waiter, stack, glue_data_mapper_factory,
                                     data_loader, fargate_queue, queue_reader):
    # Arrange
    mapper = glue_data_mapper_factory("test", partition_keys=["year"], partitions=[["2019"]])
    object_key = "test/2019/basic.parquet"
    data_loader("basic.parquet", object_key)
    # Act
    execution_arn = sf_client.start_execution(
        stateMachineArn=stack["AthenaStateMachineArn"],
        input=json.dumps({
          "Database": mapper["QueryExecutorParameters"]["Database"],
          "Table": mapper["QueryExecutorParameters"]["Database"],
          "Columns": [{"Column": "customer_id", "MatchIds": ["12345"]}],
          "PartitionKeys": [{"Key": "year", "Value": "2019"}]
        })
    )["executionArn"]
    try:
        execution_waiter.wait(executionArn=execution_arn)

    except WaiterError as e:
        pytest.fail("Error waiting for execution to enter success state: {}".format(str(e)))

    messages = queue_reader(fargate_queue)
    # Assert
    assert len(messages) == 1
    body = json.loads(messages[0].body)
    assert [
        {"Column": "customer_id", "MatchIds": ["12345"]}
    ] == body["Columns"]
    assert "s3://{}/{}".format(dummy_lake["bucket_name"], object_key) == body["Object"]

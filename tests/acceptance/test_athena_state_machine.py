import json
import logging
import pytest
from botocore.exceptions import WaiterError

logger = logging.getLogger()

pytestmark = [pytest.mark.acceptance, pytest.mark.state_machine, pytest.mark.usefixtures("empty_lake")]


def test_it_queries_unpartitioned_data(sf_client, dummy_lake, execution_waiter, stack, glue_data_mapper_factory,
                                       data_loader):
    # Arrange
    mapper = glue_data_mapper_factory("test")
    object_key = "test/basic.parquet"
    data_loader("basic.parquet", object_key)
    # Act
    execution_arn = sf_client.start_execution(
        stateMachineArn=stack["AthenaStateMachineArn"],
        input=json.dumps({
          "DataMappers": [
            mapper
          ],
          "DeletionQueue": [
            {
              "MatchId": "12345"
            }
          ]
        })
    )["executionArn"]
    try:
        execution_waiter.wait(executionArn=execution_arn)
    except WaiterError as e:
        pytest.fail("Error waiting for execution to enter success state: {}".format(str(e)))

    output = json.loads(sf_client.describe_execution(executionArn=execution_arn)["output"])
    # Assert
    assert len(output[0]["Columns"]) == 1
    assert [
        {"Column": "customer_id", "MatchIds": ["12345"]}
    ] == output[0]["Columns"]
    assert ["s3://{}/{}".format(dummy_lake["bucket_name"], object_key)] == output[0]["Objects"]


def test_it_queries_partitioned_data(sf_client, dummy_lake, execution_waiter, stack, glue_data_mapper_factory,
                                     data_loader):
    # Arrange
    mapper = glue_data_mapper_factory("test", partition_keys=["year"], partitions=[["2019"]])
    object_key = "test/2019/basic.parquet"
    data_loader("basic.parquet", object_key)
    # Act
    execution_arn = sf_client.start_execution(
        stateMachineArn=stack["AthenaStateMachineArn"],
        input=json.dumps({
          "DataMappers": [
            mapper
          ],
          "DeletionQueue": [
            {
              "MatchId": "12345"
            }
          ]
        })
    )["executionArn"]
    try:
        execution_waiter.wait(executionArn=execution_arn)
    except WaiterError as e:
        pytest.fail("Error waiting for execution to enter success state: {}".format(str(e)))

    output = json.loads(sf_client.describe_execution(executionArn=execution_arn)["output"])
    # Assert
    assert len(output[0]["Columns"]) == 1
    assert [
        {"Column": "customer_id", "MatchIds": ["12345"]}
    ] == output[0]["Columns"]
    assert ["s3://{}/{}".format(dummy_lake["bucket_name"], object_key)] == output[0]["Objects"]


def test_it_handles_multiple_mappers(sf_client, dummy_lake, execution_waiter, stack, glue_data_mapper_factory,
                                     data_loader):
    # Arrange
    mapper_a = glue_data_mapper_factory("a", database="dba", table="tablea")
    mapper_b = glue_data_mapper_factory("b", database="dbb", table="tableb")
    object_key_a = "a/basic.parquet"
    object_key_b = "b/basic.parquet"
    data_loader("basic.parquet", object_key_a)
    data_loader("basic.parquet", object_key_b)
    # Act
    execution_arn = sf_client.start_execution(
        stateMachineArn=stack["AthenaStateMachineArn"],
        input=json.dumps({
          "DataMappers": [
            mapper_a,
            mapper_b
          ],
          "DeletionQueue": [
            {
              "MatchId": "12345"
            }
          ]
        })
    )["executionArn"]
    try:
        execution_waiter.wait(executionArn=execution_arn)
    except WaiterError as e:
        pytest.fail("Error waiting for execution to enter success state: {}".format(str(e)))

    output = json.loads(sf_client.describe_execution(executionArn=execution_arn)["output"])
    # Assert
    assert len(output) == 2
    all_paths = output[0]["Objects"] + output[1]["Objects"]
    assert [
        "s3://{}/{}".format(dummy_lake["bucket_name"], object_key_a),
        "s3://{}/{}".format(dummy_lake["bucket_name"], object_key_b),
   ] == all_paths

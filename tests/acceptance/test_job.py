import tempfile
import uuid

import mock
import pytest

from tests.acceptance import query_parquet_file

pytestmark = [pytest.mark.acceptance, pytest.mark.api, pytest.mark.jobs, pytest.mark.usefixtures("empty_jobs")]


@pytest.mark.auth
def test_auth(api_client, jobs_endpoint):
    headers = {"Authorization": None}
    assert 401 == api_client.get("{}/{}".format(jobs_endpoint, "a"), headers=headers).status_code
    assert 401 == api_client.get(jobs_endpoint, headers=headers).status_code
    assert 401 == api_client.get("{}/{}/events".format(jobs_endpoint, "a"), headers=headers).status_code


def test_it_gets_jobs(api_client, jobs_endpoint, job_factory, stack, job_table, job_exists_waiter):
    # Arrange
    job_id = job_factory()["Id"]
    job_exists_waiter.wait(TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}})
    # Act
    response = api_client.get("{}/{}".format(jobs_endpoint, job_id))
    response_body = response.json()
    # Assert
    assert response.status_code == 200
    assert {
        "Id": job_id,
        "Sk": job_id,
        "Type": "Job",
        "JobStatus": mock.ANY,
        "GSIBucket": mock.ANY,
        "CreatedAt": mock.ANY,
        "DeletionQueueItems": mock.ANY,
        "AthenaConcurrencyLimit": mock.ANY,
        "DeletionTasksMaxNumber": mock.ANY,
        "QueryExecutionWaitSeconds": mock.ANY,
        "QueryQueueWaitSeconds": mock.ANY,
        "ForgetQueueWaitSeconds": mock.ANY,
    } == response_body
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]


def test_it_handles_unknown_jobs(api_client, jobs_endpoint, stack):
    # Arrange
    job_id = "invalid"
    # Act
    response = api_client.get("{}/{}".format(jobs_endpoint, job_id))
    # Assert
    assert response.status_code == 404
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]


def test_it_lists_jobs_by_date(api_client, jobs_endpoint, job_factory, stack, job_table, job_exists_waiter):
    # Arrange
    job_id_1 = job_factory(job_id=str(uuid.uuid4()), created_at=1576861489)["Id"]
    job_id_2 = job_factory(job_id=str(uuid.uuid4()), created_at=1576861490)["Id"]
    job_exists_waiter.wait(TableName=job_table.name, Key={"Id": {"S": job_id_1}, "Sk": {"S": job_id_1}})
    job_exists_waiter.wait(TableName=job_table.name, Key={"Id": {"S": job_id_2}, "Sk": {"S": job_id_2}})
    # Act
    response = api_client.get(jobs_endpoint)
    response_body = response.json()
    # Assert
    assert response.status_code == 200
    assert response_body["Jobs"][0]["CreatedAt"] >= response_body["Jobs"][1]["CreatedAt"]
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]


def test_it_lists_job_events_by_date(api_client, jobs_endpoint, job_factory, stack, job_table, job_finished_waiter):
    # Arrange
    job_id = str(uuid.uuid4())
    job_id = job_factory(job_id=job_id, created_at=1576861489)["Id"]
    job_finished_waiter.wait(TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}})
    # Act
    response = api_client.get("{}/{}/events".format(jobs_endpoint, job_id))
    response_body = response.json()
    job_events = response_body["JobEvents"]
    # Assert
    assert response.status_code == 200
    assert len(job_events) > 0
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]
    assert all(job_events[i]["CreatedAt"] <= job_events[i+1]["CreatedAt"] for i in range(len(job_events)-1))


def test_it_runs_for_happy_path(del_queue_factory, job_factory, dummy_lake, glue_data_mapper_factory, data_loader,
                                job_complete_waiter, job_table):
    # Generate a parquet file and add it to the lake
    glue_data_mapper_factory("test", partition_keys=["year", "month", "day"], partitions=[["2019", "08", "20"]])
    item = del_queue_factory("12345")
    object_key = "test/2019/08/20/test.parquet"
    data_loader("basic.parquet", object_key)
    bucket = dummy_lake["bucket"]
    job_id = job_factory(del_queue_items=[item])["Id"]
    # Act
    job_complete_waiter.wait(TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}})
    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_fileobj(object_key, tmp)
    assert "COMPLETED" == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    assert 0 == len(query_parquet_file(tmp, "customer_id", "12345"))
    assert 2 == len(list(bucket.object_versions.filter(Prefix=object_key)))


def test_it_runs_for_unpartitioned_data(del_queue_factory, job_factory, dummy_lake, glue_data_mapper_factory,
                                        data_loader, job_complete_waiter, job_table):
    # Generate a parquet file and add it to the lake
    glue_data_mapper_factory("test")
    item = del_queue_factory("12345")
    object_key = "test/test.parquet"
    data_loader("basic.parquet", object_key)
    bucket = dummy_lake["bucket"]
    job_id = job_factory(del_queue_items=[item])["Id"]
    # Act
    job_complete_waiter.wait(TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}})
    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_fileobj(object_key, tmp)
    assert "COMPLETED" == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    assert 0 == len(query_parquet_file(tmp, "customer_id", "12345"))


def test_it_does_not_permit_unversioned_buckets(
        del_queue_factory, job_factory, dummy_lake, glue_data_mapper_factory,
        data_loader, job_finished_waiter, job_table, s3_resource):
    try:
        s3_resource.BucketVersioning(dummy_lake["bucket_name"]).suspend()
        # Generate a parquet file and add it to the lake
        glue_data_mapper_factory("test", partition_keys=["year", "month", "day"], partitions=[["2019", "08", "20"]])
        item = del_queue_factory("12345")
        object_key = "test/2019/08/20/test.parquet"
        data_loader("basic.parquet", object_key)
        bucket = dummy_lake["bucket"]
        job_id = job_factory(del_queue_items=[item], delete_previous_versions=False)["Id"]
        # Act
        job_finished_waiter.wait(TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}})
        # Assert
        tmp = tempfile.NamedTemporaryFile()
        bucket.download_fileobj(object_key, tmp)
        assert "FORGET_PARTIALLY_FAILED" == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
        assert 1 == len(query_parquet_file(tmp, "customer_id", "12345"))
    finally:
        s3_resource.BucketVersioning(dummy_lake["bucket_name"]).enable()


def test_it_retains_settings(del_queue_factory, job_factory, dummy_lake, glue_data_mapper_factory, data_loader,
                             job_finished_waiter, job_table):
    # Generate a parquet file and add it to the lake
    glue_data_mapper_factory("test", partition_keys=["year", "month", "day"], partitions=[["2019", "08", "20"]])
    item = del_queue_factory("12345")
    object_key = "test/2019/08/20/test.parquet"
    data_loader("basic.parquet", object_key, Metadata={"foo": "bar"}, CacheControl="cache")
    bucket = dummy_lake["bucket"]
    job_id = job_factory(del_queue_items=[item], delete_previous_versions=False)["Id"]
    # Act
    job_finished_waiter.wait(TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}})
    # Assert
    assert {"foo": "bar"} == bucket.Object(object_key).metadata
    assert "cache" == bucket.Object(object_key).cache_control


def test_it_executes_successfully_for_empty_queue(job_factory, job_finished_waiter, job_table):
    # Arrange
    job_id = job_factory()["Id"]
    # Act
    job_finished_waiter.wait(TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}})
    # Assert
    assert "COMPLETED" == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]


def test_it_supports_data_access_roles(del_queue_factory, job_factory, dummy_lake, glue_data_mapper_factory,
                                       data_loader, job_complete_waiter, job_table, data_access_role):
    if not data_access_role:
        pytest.skip('Skipped due to missing data access role')
    # Generate a parquet file and add it to the lake
    glue_data_mapper_factory("test", partition_keys=["year", "month", "day"], partitions=[["2019", "08", "20"]],
                             delete_old_versions=False, role_arn=data_access_role["Arn"])
    item = del_queue_factory("12345")
    object_key = "test/2019/08/20/test.parquet"
    data_loader("basic.parquet", object_key)
    bucket = dummy_lake["bucket"]
    job_id = job_factory(del_queue_items=[item])["Id"]
    # Act
    job_complete_waiter.wait(TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}})
    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_fileobj(object_key, tmp)
    assert "COMPLETED" == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    assert 0 == len(query_parquet_file(tmp, "customer_id", "12345"))


def test_it_deletes_old_versions(del_queue_factory, job_factory, dummy_lake, glue_data_mapper_factory, data_loader,
                                 job_complete_waiter, job_table, data_access_role):
    if not data_access_role:
        pytest.skip('Skipped due to missing data access role')
    # Generate a parquet file and add it to the lake
    glue_data_mapper_factory("test", partition_keys=["year", "month", "day"], partitions=[["2019", "08", "20"]],
                             delete_old_versions=True, role_arn=data_access_role["Arn"])
    item = del_queue_factory("12345")
    object_key = "test/2019/08/20/test.parquet"
    bucket = dummy_lake["bucket"]
    # Create the object, add a deletion marker, then recreate it
    data_loader("basic.parquet", object_key)
    bucket.Object("basic.parquet").delete()
    data_loader("basic.parquet", object_key)
    job_id = job_factory(del_queue_items=[item])["Id"]
    # Act
    job_complete_waiter.wait(TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}})
    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_fileobj(object_key, tmp)
    assert "COMPLETED" == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    assert 0 == len(query_parquet_file(tmp, "customer_id", "12345"))
    assert 1 == len(list(bucket.object_versions.filter(Prefix=object_key)))

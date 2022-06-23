import json
import tempfile
import uuid

import mock
import pytest
from decimal import Decimal

from tests.acceptance import query_json_file, query_parquet_file, download_and_decrypt

pytestmark = [
    pytest.mark.acceptance_cognito,
    pytest.mark.jobs,
    pytest.mark.usefixtures("empty_jobs"),
]


@pytest.mark.auth
@pytest.mark.api
def test_auth(api_client_cognito, jobs_endpoint):
    headers = {"Authorization": None}
    assert (
        401
        == api_client_cognito.get(
            "{}/{}".format(jobs_endpoint, "a"), headers=headers
        ).status_code
    )
    assert 401 == api_client_cognito.get(jobs_endpoint, headers=headers).status_code
    assert (
        401
        == api_client_cognito.get(
            "{}/{}/events".format(jobs_endpoint, "a"), headers=headers
        ).status_code
    )


@pytest.mark.api
def test_it_gets_jobs(
    api_client_cognito, jobs_endpoint, job_factory, stack, job_table, job_exists_waiter
):
    # Arrange
    job_id = job_factory()["Id"]
    job_exists_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Act
    response = api_client_cognito.get("{}/{}".format(jobs_endpoint, job_id))
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
        "AthenaConcurrencyLimit": mock.ANY,
        "AthenaQueryMaxRetries": mock.ANY,
        "DeletionTasksMaxNumber": mock.ANY,
        "QueryExecutionWaitSeconds": mock.ANY,
        "QueryQueueWaitSeconds": mock.ANY,
        "ForgetQueueWaitSeconds": mock.ANY,
    } == response_body
    assert (
        response.headers.get("Access-Control-Allow-Origin")
        == stack["APIAccessControlAllowOriginHeader"]
    )


@pytest.mark.api
def test_it_handles_unknown_jobs(api_client_cognito, jobs_endpoint, stack):
    # Arrange
    job_id = "invalid"
    # Act
    response = api_client_cognito.get("{}/{}".format(jobs_endpoint, job_id))
    # Assert
    assert response.status_code == 404
    assert (
        response.headers.get("Access-Control-Allow-Origin")
        == stack["APIAccessControlAllowOriginHeader"]
    )


@pytest.mark.api
def test_it_lists_jobs_by_date(
    api_client_cognito, jobs_endpoint, job_factory, stack, job_table, job_exists_waiter
):
    # Arrange
    job_id_1 = job_factory(job_id=str(uuid.uuid4()), created_at=1576861489)["Id"]
    job_id_2 = job_factory(job_id=str(uuid.uuid4()), created_at=1576861490)["Id"]
    job_exists_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id_1}, "Sk": {"S": job_id_1}}
    )
    job_exists_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id_2}, "Sk": {"S": job_id_2}}
    )
    # Act
    response = api_client_cognito.get(jobs_endpoint)
    response_body = response.json()
    # Assert
    assert response.status_code == 200
    assert (
        response_body["Jobs"][0]["CreatedAt"] >= response_body["Jobs"][1]["CreatedAt"]
    )
    assert (
        response.headers.get("Access-Control-Allow-Origin")
        == stack["APIAccessControlAllowOriginHeader"]
    )


@pytest.mark.api
def test_it_returns_summary_fields_in_list(
    api_client_cognito, jobs_endpoint, job_factory, job_table, job_finished_waiter
):
    # Arrange
    job_id = job_factory(job_id=str(uuid.uuid4()), created_at=1576861489)["Id"]
    job_finished_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Act
    response = api_client_cognito.get(jobs_endpoint)
    response_body = response.json()
    # Assert
    assert response.status_code == 200
    for job in response_body["Jobs"]:
        assert all(
            [
                k in job
                for k in [
                    "Id",
                    "CreatedAt",
                    "JobStatus",
                    "JobFinishTime",
                    "JobStartTime",
                    "TotalObjectRollbackFailedCount",
                    "TotalObjectUpdatedCount",
                    "TotalObjectUpdateSkippedCount",
                    "TotalObjectUpdateFailedCount",
                    "TotalQueryCount",
                    "TotalQueryScannedInBytes",
                    "TotalQuerySucceededCount",
                    "TotalQueryTimeInMillis",
                ]
            ]
        )


@pytest.mark.api
def test_it_lists_job_events_by_date(
    api_client_cognito,
    jobs_endpoint,
    job_factory,
    stack,
    job_table,
    job_finished_waiter,
):
    # Arrange
    job_id = str(uuid.uuid4())
    job_id = job_factory(job_id=job_id, created_at=1576861489)["Id"]
    job_finished_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Act
    response = api_client_cognito.get("{}/{}/events".format(jobs_endpoint, job_id))
    response_body = response.json()
    # Assert
    assert response.status_code == 200
    job_events = response_body["JobEvents"]
    assert len(job_events) > 0
    assert (
        response.headers.get("Access-Control-Allow-Origin")
        == stack["APIAccessControlAllowOriginHeader"]
    )
    assert all(
        job_events[i]["CreatedAt"] <= job_events[i + 1]["CreatedAt"]
        for i in range(len(job_events) - 1)
    )


@pytest.mark.api
def test_it_filters_job_events_by_event_name(
    api_client_cognito,
    jobs_endpoint,
    job_factory,
    stack,
    job_table,
    job_finished_waiter,
):
    # Arrange
    job_id = str(uuid.uuid4())
    job_id = job_factory(job_id=job_id, created_at=1576861489)["Id"]
    job_finished_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Act
    response = api_client_cognito.get(
        "{}/{}/events?filter=EventName%3DFindPhaseStarted".format(jobs_endpoint, job_id)
    )
    response_body = response.json()
    job_events = response_body["JobEvents"]
    # Assert
    assert response.status_code == 200
    assert len(job_events) == 1
    assert "FindPhaseStarted" == job_events[0]["EventName"]
    assert (
        response.headers.get("Access-Control-Allow-Origin")
        == stack["APIAccessControlAllowOriginHeader"]
    )


def test_it_runs_for_parquet_happy_path(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    data_loader,
    job_complete_waiter,
    job_table,
):
    # Arrange
    glue_data_mapper_factory(
        "test",
        partition_keys=["year", "month", "day"],
        partitions=[["2019", "08", "20"]],
    )
    item = del_queue_factory("12345")
    object_key = "test/2019/08/20/test.parquet"
    data_loader(
        "basic.parquet", object_key, Metadata={"foo": "bar"}, CacheControl="cache"
    )
    bucket = dummy_lake["bucket"]
    job_id = job_factory(del_queue_items=[item])["Id"]
    # Act
    job_complete_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_fileobj(object_key, tmp)
    assert (
        "COMPLETED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )
    assert 0 == len(query_parquet_file(tmp, "customer_id", "12345"))
    assert 1 == len(query_parquet_file(tmp, "customer_id", "23456"))
    assert 1 == len(query_parquet_file(tmp, "customer_id", "34567"))
    assert 2 == len(list(bucket.object_versions.filter(Prefix=object_key)))
    assert {"foo": "bar"} == bucket.Object(object_key).metadata
    assert "cache" == bucket.Object(object_key).cache_control


def test_it_runs_for_parquet_cse_kms(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    encrypted_data_loader,
    job_complete_waiter,
    job_table,
    kms_factory,
    kms_client,
):
    # Arrange
    glue_data_mapper_factory(
        "test",
        partition_keys=["year", "month", "day"],
        partitions=[["2019", "08", "20"]],
        encrypted=True,
    )
    item = del_queue_factory("12345")
    encryption_key = kms_factory
    object_key_cbc = "test/2019/08/20/test_cbc.parquet"
    object_key_gcm = "test/2019/08/20/test_gcm.parquet"
    encrypted_data_loader(
        "basic.parquet",
        object_key_cbc,
        encryption_key,
        "AES/CBC/PKCS5Padding",
        CacheControl="cache",
    )
    encrypted_data_loader(
        "basic.parquet",
        object_key_gcm,
        encryption_key,
        "AES/GCM/NoPadding",
        CacheControl="cache",
    )
    bucket = dummy_lake["bucket"]
    job_id = job_factory(del_queue_items=[item])["Id"]
    # Act
    job_complete_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    decrypted_cbc, metadata_cbc = download_and_decrypt(
        bucket, object_key_cbc, kms_client
    )
    decrypted_gcm, metadata_gcm = download_and_decrypt(
        bucket, object_key_gcm, kms_client
    )
    tmp_cbc = tempfile.NamedTemporaryFile()
    tmp_gcm = tempfile.NamedTemporaryFile()
    open(tmp_cbc.name, "wb").write(decrypted_cbc)
    open(tmp_gcm.name, "wb").write(decrypted_gcm)
    # Assert
    assert (
        "COMPLETED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )

    assert 0 == len(query_parquet_file(tmp_cbc, "customer_id", "12345"))
    assert 1 == len(query_parquet_file(tmp_cbc, "customer_id", "23456"))
    assert 1 == len(query_parquet_file(tmp_cbc, "customer_id", "34567"))
    assert metadata_cbc["x-amz-cek-alg"] == "AES/CBC/PKCS5Padding"
    assert 2 == len(list(bucket.object_versions.filter(Prefix=object_key_cbc)))
    assert "cache" == bucket.Object(object_key_cbc).cache_control

    assert 0 == len(query_parquet_file(tmp_gcm, "customer_id", "12345"))
    assert 1 == len(query_parquet_file(tmp_gcm, "customer_id", "23456"))
    assert 1 == len(query_parquet_file(tmp_gcm, "customer_id", "34567"))
    assert metadata_gcm["x-amz-cek-alg"] == "AES/GCM/NoPadding"
    assert 2 == len(list(bucket.object_versions.filter(Prefix=object_key_gcm)))
    assert "cache" == bucket.Object(object_key_gcm).cache_control


def test_it_runs_for_parquet_backwards_compatible_matches(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    data_loader,
    job_complete_waiter,
    job_table,
):
    # Arrange
    glue_data_mapper_factory(
        "test",
        partition_keys=["year", "month", "day"],
        partitions=[["2019", "08", "20"]],
    )
    # MatchId Type was introduced in 0.19 only and it should default to Simple
    item = del_queue_factory("12345", matchid_type=None)
    object_key = "test/2019/08/20/test.parquet"
    data_loader(
        "basic.parquet", object_key, Metadata={"foo": "bar"}, CacheControl="cache"
    )
    bucket = dummy_lake["bucket"]
    job_id = job_factory(del_queue_items=[item])["Id"]
    # Act
    job_complete_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_fileobj(object_key, tmp)
    assert (
        "COMPLETED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )
    assert 0 == len(query_parquet_file(tmp, "customer_id", "12345"))
    assert 1 == len(query_parquet_file(tmp, "customer_id", "23456"))
    assert 1 == len(query_parquet_file(tmp, "customer_id", "34567"))
    assert 2 == len(list(bucket.object_versions.filter(Prefix=object_key)))
    assert {"foo": "bar"} == bucket.Object(object_key).metadata
    assert "cache" == bucket.Object(object_key).cache_control


def test_it_runs_for_parquet_composite_matches(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    data_loader,
    job_complete_waiter,
    job_table,
):
    # Arrange
    glue_data_mapper_factory(
        "test",
        partition_keys=["year", "month", "day"],
        partitions=[["2019", "08", "20"]],
    )
    item = del_queue_factory(
        [
            {"Column": "user_info.personal_information.first_name", "Value": "John"},
            {"Column": "user_info.personal_information.last_name", "Value": "Doe"},
        ],
        "id123",
        matchid_type="Composite",
        data_mappers=["test"],
    )
    object_key = "test/2019/08/20/test.parquet"
    data_loader(
        "basic.parquet", object_key, Metadata={"foo": "bar"}, CacheControl="cache"
    )
    bucket = dummy_lake["bucket"]
    job_id = job_factory(del_queue_items=[item])["Id"]
    # Act
    job_complete_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_fileobj(object_key, tmp)
    assert (
        "COMPLETED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )
    assert 0 == len(query_parquet_file(tmp, "customer_id", "12345"))
    assert 1 == len(query_parquet_file(tmp, "customer_id", "23456"))
    assert 1 == len(query_parquet_file(tmp, "customer_id", "34567"))
    assert 2 == len(list(bucket.object_versions.filter(Prefix=object_key)))
    assert {"foo": "bar"} == bucket.Object(object_key).metadata
    assert "cache" == bucket.Object(object_key).cache_control


def test_it_runs_for_parquet_mixed_matches(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    data_loader,
    job_complete_waiter,
    job_table,
):
    # Arrange
    glue_data_mapper_factory(
        "test",
        partition_keys=["year", "month", "day"],
        partitions=[["2019", "08", "20"]],
    )
    composite_item = del_queue_factory(
        [
            {"Column": "user_info.personal_information.first_name", "Value": "John"},
            {"Column": "user_info.personal_information.last_name", "Value": "Doe"},
        ],
        "id123",
        matchid_type="Composite",
        data_mappers=["test"],
    )
    simple_item = del_queue_factory("23456", "id234")
    object_key = "test/2019/08/20/test.parquet"
    data_loader(
        "basic.parquet", object_key, Metadata={"foo": "bar"}, CacheControl="cache"
    )
    bucket = dummy_lake["bucket"]
    job_id = job_factory(del_queue_items=[composite_item, simple_item])["Id"]
    # Act
    job_complete_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_fileobj(object_key, tmp)
    assert (
        "COMPLETED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )
    assert 0 == len(query_parquet_file(tmp, "customer_id", "12345"))
    assert 0 == len(query_parquet_file(tmp, "customer_id", "23456"))
    assert 1 == len(query_parquet_file(tmp, "customer_id", "34567"))
    assert 2 == len(list(bucket.object_versions.filter(Prefix=object_key)))
    assert {"foo": "bar"} == bucket.Object(object_key).metadata
    assert "cache" == bucket.Object(object_key).cache_control


def test_it_runs_for_json_happy_path(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    data_loader,
    job_complete_waiter,
    job_table,
):
    # Arrange
    glue_data_mapper_factory(
        "test",
        partition_keys=["year", "month", "day"],
        partitions=[["2019", "08", "20"]],
        fmt="json",
    )
    item = del_queue_factory("12345")
    object_key = "test/2019/08/20/test.json"
    data_loader("basic.json", object_key, Metadata={"foo": "bar"}, CacheControl="cache")
    bucket = dummy_lake["bucket"]
    job_id = job_factory(del_queue_items=[item])["Id"]
    # Act
    job_complete_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_file(object_key, tmp.name)
    assert (
        "COMPLETED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )
    assert 0 == len(query_json_file(tmp.name, "customer_id", "12345"))
    assert 1 == len(query_json_file(tmp.name, "customer_id", "23456"))
    assert 1 == len(query_json_file(tmp.name, "customer_id", "34567"))
    assert 2 == len(list(bucket.object_versions.filter(Prefix=object_key)))
    assert {"foo": "bar"} == bucket.Object(object_key).metadata
    assert "cache" == bucket.Object(object_key).cache_control


def test_it_runs_for_json_cse_kms(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    encrypted_data_loader,
    job_complete_waiter,
    job_table,
    kms_factory,
    kms_client,
):
    # Arrange
    glue_data_mapper_factory(
        "test",
        partition_keys=["year", "month", "day"],
        partitions=[["2019", "08", "20"]],
        fmt="json",
        encrypted=True,
    )
    item = del_queue_factory("12345")
    encryption_key = kms_factory
    object_key_cbc = "test/2019/08/20/test_cbc.json"
    object_key_gcm = "test/2019/08/20/test_gcm.json"
    encrypted_data_loader(
        "basic.json",
        object_key_cbc,
        encryption_key,
        "AES/CBC/PKCS5Padding",
        CacheControl="cache",
    )
    encrypted_data_loader(
        "basic.json",
        object_key_gcm,
        encryption_key,
        "AES/GCM/NoPadding",
        CacheControl="cache",
    )
    bucket = dummy_lake["bucket"]
    job_id = job_factory(del_queue_items=[item])["Id"]
    # Act
    job_complete_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    decrypted_cbc, metadata_cbc = download_and_decrypt(
        bucket, object_key_cbc, kms_client
    )
    decrypted_gcm, metadata_gcm = download_and_decrypt(
        bucket, object_key_gcm, kms_client
    )
    tmp_cbc = tempfile.NamedTemporaryFile()
    tmp_gcm = tempfile.NamedTemporaryFile()
    open(tmp_cbc.name, "wb").write(decrypted_cbc)
    open(tmp_gcm.name, "wb").write(decrypted_gcm)
    # Assert
    assert (
        "COMPLETED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )

    assert 0 == len(query_json_file(tmp_cbc.name, "customer_id", "12345"))
    assert 1 == len(query_json_file(tmp_cbc.name, "customer_id", "23456"))
    assert 1 == len(query_json_file(tmp_cbc.name, "customer_id", "34567"))
    assert metadata_cbc["x-amz-cek-alg"] == "AES/CBC/PKCS5Padding"
    assert 2 == len(list(bucket.object_versions.filter(Prefix=object_key_cbc)))
    assert "cache" == bucket.Object(object_key_cbc).cache_control

    assert 0 == len(query_json_file(tmp_gcm.name, "customer_id", "12345"))
    assert 1 == len(query_json_file(tmp_gcm.name, "customer_id", "23456"))
    assert 1 == len(query_json_file(tmp_gcm.name, "customer_id", "34567"))
    assert metadata_gcm["x-amz-cek-alg"] == "AES/GCM/NoPadding"
    assert 2 == len(list(bucket.object_versions.filter(Prefix=object_key_gcm)))
    assert "cache" == bucket.Object(object_key_gcm).cache_control


def test_it_runs_for_json_composite_matches(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    data_loader,
    job_complete_waiter,
    job_table,
):
    # Arrange
    glue_data_mapper_factory(
        "test",
        partition_keys=["year", "month", "day"],
        partitions=[["2019", "08", "20"]],
        fmt="json",
    )
    composite_item = del_queue_factory(
        [
            {"Column": "user_info.personal_information.first_name", "Value": "John"},
            {"Column": "user_info.personal_information.last_name", "Value": "Doe"},
        ],
        "id123",
        matchid_type="Composite",
        data_mappers=["test"],
    )
    object_key = "test/2019/08/20/test.json"
    data_loader("basic.json", object_key, Metadata={"foo": "bar"}, CacheControl="cache")
    bucket = dummy_lake["bucket"]
    job_id = job_factory(del_queue_items=[composite_item])["Id"]
    # Act
    job_complete_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_file(object_key, tmp.name)
    assert (
        "COMPLETED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )
    assert 0 == len(query_json_file(tmp.name, "customer_id", "12345"))
    assert 1 == len(query_json_file(tmp.name, "customer_id", "23456"))
    assert 1 == len(query_json_file(tmp.name, "customer_id", "34567"))
    assert 2 == len(list(bucket.object_versions.filter(Prefix=object_key)))
    assert {"foo": "bar"} == bucket.Object(object_key).metadata
    assert "cache" == bucket.Object(object_key).cache_control


def test_it_runs_for_json_mixed_matches(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    data_loader,
    job_complete_waiter,
    job_table,
):
    # Arrange
    glue_data_mapper_factory(
        "test",
        partition_keys=["year", "month", "day"],
        partitions=[["2019", "08", "20"]],
        fmt="json",
    )
    composite_item = del_queue_factory(
        [
            {"Column": "user_info.personal_information.first_name", "Value": "John"},
            {"Column": "user_info.personal_information.last_name", "Value": "Doe"},
        ],
        "id123",
        matchid_type="Composite",
        data_mappers=["test"],
    )
    simple_item = del_queue_factory("23456", "id234")
    object_key = "test/2019/08/20/test.json"
    data_loader("basic.json", object_key, Metadata={"foo": "bar"}, CacheControl="cache")
    bucket = dummy_lake["bucket"]
    job_id = job_factory(del_queue_items=[composite_item, simple_item])["Id"]
    # Act
    job_complete_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_file(object_key, tmp.name)
    assert (
        "COMPLETED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )
    assert 0 == len(query_json_file(tmp.name, "customer_id", "12345"))
    assert 0 == len(query_json_file(tmp.name, "customer_id", "23456"))
    assert 1 == len(query_json_file(tmp.name, "customer_id", "34567"))
    assert 2 == len(list(bucket.object_versions.filter(Prefix=object_key)))
    assert {"foo": "bar"} == bucket.Object(object_key).metadata
    assert "cache" == bucket.Object(object_key).cache_control


def test_it_runs_for_unpartitioned_data(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    data_loader,
    job_complete_waiter,
    job_table,
):
    # Arrange
    glue_data_mapper_factory("test")
    item = del_queue_factory("12345")
    object_key = "test/test.parquet"
    data_loader(
        "basic.parquet", object_key, Metadata={"foo": "bar"}, CacheControl="cache"
    )
    bucket = dummy_lake["bucket"]
    job_id = job_factory(del_queue_items=[item])["Id"]
    # Act
    job_complete_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_fileobj(object_key, tmp)
    assert (
        "COMPLETED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )
    assert 0 == len(query_parquet_file(tmp, "customer_id", "12345"))
    assert {"foo": "bar"} == bucket.Object(object_key).metadata
    assert "cache" == bucket.Object(object_key).cache_control


def test_it_runs_for_complex_types(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    data_loader,
    job_complete_waiter,
    job_table,
):
    # Arrange
    glue_data_mapper_factory(
        "test", column_identifiers=["user_info.personal_information.first_name"]
    )
    item = del_queue_factory("John")
    object_key = "test/test.parquet"
    data_loader(
        "basic.parquet", object_key, Metadata={"foo": "bar"}, CacheControl="cache"
    )
    bucket = dummy_lake["bucket"]
    job_id = job_factory(del_queue_items=[item])["Id"]
    # Act
    job_complete_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_fileobj(object_key, tmp)
    assert (
        "COMPLETED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )
    assert 0 == len(query_parquet_file(tmp, "customer_id", "12345"))
    assert {"foo": "bar"} == bucket.Object(object_key).metadata
    assert "cache" == bucket.Object(object_key).cache_control


def test_it_runs_for_partitioned_data_with_non_string_partitions(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    data_loader,
    job_complete_waiter,
    job_table,
):
    # Arrange
    glue_data_mapper_factory(
        "test",
        partition_keys=["year", "month", "day"],
        partitions=[["2019", "10", "20"]],
        partition_key_types="int",
    )
    item = del_queue_factory("12345")
    object_key = "test/2019/10/20/test.parquet"
    data_loader(
        "basic.parquet", object_key, Metadata={"foo": "bar"}, CacheControl="cache"
    )
    bucket = dummy_lake["bucket"]
    job_id = job_factory(del_queue_items=[item])["Id"]
    # Act
    job_complete_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_fileobj(object_key, tmp)
    assert (
        "COMPLETED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )
    assert 0 == len(query_parquet_file(tmp, "customer_id", "12345"))
    assert 1 == len(query_parquet_file(tmp, "customer_id", "23456"))
    assert 1 == len(query_parquet_file(tmp, "customer_id", "34567"))
    assert 2 == len(list(bucket.object_versions.filter(Prefix=object_key)))
    assert {"foo": "bar"} == bucket.Object(object_key).metadata
    assert "cache" == bucket.Object(object_key).cache_control


def test_it_runs_for_caselowered_identifier_parquet(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    data_loader,
    job_complete_waiter,
    job_table,
):
    # Arrange
    glue_data_mapper_factory("test", column_identifiers=["customerid"])
    item = del_queue_factory(12345)
    object_key = "test/test.parquet"
    data_loader(
        "basic.parquet", object_key, Metadata={"foo": "bar"}, CacheControl="cache"
    )
    bucket = dummy_lake["bucket"]
    job_id = job_factory(del_queue_items=[item])["Id"]
    # Act
    job_complete_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_fileobj(object_key, tmp)
    assert (
        "COMPLETED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )
    assert 0 == len(query_parquet_file(tmp, "customerId", 12345))
    assert {"foo": "bar"} == bucket.Object(object_key).metadata
    assert "cache" == bucket.Object(object_key).cache_control


def test_it_runs_for_decimal_identifier_parquet(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    data_loader,
    job_complete_waiter,
    job_table,
):
    # Arrange
    glue_data_mapper_factory("test", column_identifiers=["customer_id_decimal"])
    item = del_queue_factory("123.450")
    object_key = "test/test.parquet"
    data_loader(
        "basic.parquet", object_key, Metadata={"foo": "bar"}, CacheControl="cache"
    )
    bucket = dummy_lake["bucket"]
    job_id = job_factory(del_queue_items=[item])["Id"]
    # Act
    job_complete_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_fileobj(object_key, tmp)
    assert (
        "COMPLETED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )
    assert 0 == len(query_parquet_file(tmp, "customer_id_decimal", Decimal("123.450")))
    assert {"foo": "bar"} == bucket.Object(object_key).metadata
    assert "cache" == bucket.Object(object_key).cache_control


def test_it_does_not_permit_unversioned_buckets(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    data_loader,
    job_finished_waiter,
    job_table,
    s3_resource,
):
    try:
        # Arrange
        s3_resource.BucketVersioning(dummy_lake["bucket_name"]).suspend()
        glue_data_mapper_factory(
            "test",
            partition_keys=["year", "month", "day"],
            partitions=[["2019", "08", "20"]],
        )
        item = del_queue_factory("12345")
        object_key = "test/2019/08/20/test.parquet"
        data_loader("basic.parquet", object_key)
        bucket = dummy_lake["bucket"]
        job_id = job_factory(del_queue_items=[item], delete_previous_versions=False)[
            "Id"
        ]
        # Act
        job_finished_waiter.wait(
            TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
        )
        # Assert
        tmp = tempfile.NamedTemporaryFile()
        bucket.download_fileobj(object_key, tmp)
        assert (
            "FORGET_PARTIALLY_FAILED"
            == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
        )
        assert 1 == len(query_parquet_file(tmp, "customer_id", "12345"))
    finally:
        s3_resource.BucketVersioning(dummy_lake["bucket_name"]).enable()


def test_it_executes_successfully_for_empty_queue(
    job_factory, job_finished_waiter, job_table
):
    # Arrange
    job_id = job_factory()["Id"]
    # Act
    job_finished_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Assert
    assert (
        "COMPLETED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )


def test_it_supports_data_access_roles(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    data_loader,
    job_complete_waiter,
    job_table,
    data_access_role,
):
    # Arrange
    glue_data_mapper_factory(
        "test",
        partition_keys=["year", "month", "day"],
        partitions=[["2019", "08", "20"]],
        delete_old_versions=False,
        role_arn=data_access_role["Arn"],
    )
    item = del_queue_factory("12345")
    object_key = "test/2019/08/20/test.parquet"
    data_loader("basic.parquet", object_key)
    bucket = dummy_lake["bucket"]
    job_id = job_factory(del_queue_items=[item])["Id"]
    # Act
    job_complete_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_fileobj(object_key, tmp)
    assert (
        "COMPLETED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )
    assert 0 == len(query_parquet_file(tmp, "customer_id", "12345"))


def test_it_deletes_old_versions(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    data_loader,
    job_complete_waiter,
    job_table,
    data_access_role,
):
    # Arrange
    glue_data_mapper_factory(
        "test",
        partition_keys=["year", "month", "day"],
        partitions=[["2019", "08", "20"]],
        delete_old_versions=True,
        role_arn=data_access_role["Arn"],
    )
    item = del_queue_factory("12345")
    object_key = "test/2019/08/20/test.parquet"
    bucket = dummy_lake["bucket"]
    # Create the object, add a deletion marker, then recreate it
    data_loader("basic.parquet", object_key)
    bucket.Object("basic.parquet").delete()
    data_loader("basic.parquet", object_key)
    job_id = job_factory(del_queue_items=[item])["Id"]
    # Act
    job_complete_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_fileobj(object_key, tmp)
    assert (
        "COMPLETED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )
    assert 0 == len(query_parquet_file(tmp, "customer_id", "12345"))
    assert 1 == len(list(bucket.object_versions.filter(Prefix=object_key)))


def test_it_ignores_not_found_exceptions(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    data_loader,
    job_complete_waiter,
    job_table,
    data_access_role,
    find_phase_ended_waiter,
):
    # Arrange
    glue_data_mapper_factory(
        "test",
        partition_keys=["year", "month", "day"],
        partitions=[["2019", "08", "20"]],
        ignore_object_not_found_exceptions=True,
        role_arn=data_access_role["Arn"],
    )
    item = del_queue_factory("12345")
    object_key = "test/2019/08/20/test.parquet"
    bucket = dummy_lake["bucket"]
    data_loader("basic.parquet", object_key)
    # Start job, wait for find phase to end, delete object
    job_id = job_factory(del_queue_items=[item])["Id"]
    find_phase_ended_waiter.wait(job_id)
    bucket.Object(key=object_key).delete()
    # Act
    job_complete_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Assert
    job = job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]
    assert "COMPLETED" == job["JobStatus"]
    assert 0 == job["TotalObjectUpdatedCount"]
    assert 1 == job["TotalObjectUpdateSkippedCount"]
    assert 0 == job["TotalObjectUpdateFailedCount"]
    assert 0 == job["TotalObjectRollbackFailedCount"]


def test_it_handles_find_permission_issues(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    data_loader,
    job_finished_waiter,
    job_table,
    policy_changer,
    stack,
    arn_partition,
):
    # Arrange
    glue_data_mapper_factory(
        "test",
        partition_keys=["year", "month", "day"],
        partitions=[["2019", "08", "20"]],
    )
    item = del_queue_factory("12345")
    object_key = "test/2019/08/20/test.parquet"
    data_loader("basic.parquet", object_key)
    bucket = dummy_lake["bucket"]
    bucket_name = dummy_lake["bucket_name"]
    policy_changer(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Deny",
                    "Principal": {"AWS": [stack["AthenaExecutionRoleArn"]]},
                    "Action": "s3:*",
                    "Resource": [
                        "arn:{}:s3:::{}".format(arn_partition, bucket_name),
                        "arn:{}:s3:::{}/*".format(arn_partition, bucket_name),
                    ],
                }
            ],
        }
    )
    job_id = job_factory(del_queue_items=[item])["Id"]
    # Act
    job_finished_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Assert
    assert (
        "FIND_FAILED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )
    assert 1 == len(list(bucket.object_versions.filter(Prefix=object_key)))


def test_it_handles_forget_permission_issues(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    data_loader,
    job_finished_waiter,
    job_table,
    policy_changer,
    stack,
    arn_partition,
):
    # Arrange
    glue_data_mapper_factory(
        "test",
        partition_keys=["year", "month", "day"],
        partitions=[["2019", "08", "20"]],
    )
    item = del_queue_factory("12345")
    object_key = "test/2019/08/20/test.parquet"
    data_loader("basic.parquet", object_key)
    bucket = dummy_lake["bucket"]
    bucket_name = dummy_lake["bucket_name"]
    policy = json.loads(dummy_lake["policy"].policy)
    policy["Statement"].append(
        {
            "Effect": "Deny",
            "Principal": {"AWS": [stack["DeleteTaskRoleArn"]]},
            "Action": "s3:*",
            "Resource": [
                "arn:{}:s3:::{}".format(arn_partition, bucket_name),
                "arn:{}:s3:::{}/*".format(arn_partition, bucket_name),
            ],
        }
    )
    policy_changer(policy)
    job_id = job_factory(del_queue_items=[item])["Id"]
    # Act
    job_finished_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Assert
    assert (
        "FORGET_PARTIALLY_FAILED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )
    assert 1 == len(list(bucket.object_versions.filter(Prefix=object_key)))


def test_it_handles_forget_invalid_role(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    data_loader,
    job_finished_waiter,
    job_table,
    arn_partition,
):
    # Arrange
    glue_data_mapper_factory(
        "test",
        partition_keys=["year", "month", "day"],
        partitions=[["2019", "08", "20"]],
        role_arn="arn:{}:iam::invalid:role/DoesntExist".format(arn_partition),
    )
    item = del_queue_factory("12345")
    object_key = "test/2019/08/20/test.parquet"
    data_loader("basic.parquet", object_key)
    bucket = dummy_lake["bucket"]
    job_id = job_factory(del_queue_items=[item])["Id"]
    # Act
    job_finished_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_fileobj(object_key, tmp)
    assert (
        "FORGET_PARTIALLY_FAILED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )
    assert 1 == len(list(bucket.object_versions.filter(Prefix=object_key)))

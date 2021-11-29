import os
import re
from types import SimpleNamespace

import pytest
from mock import patch

from backend.lambdas.tasks.execute_query import handler, make_query, escape_item

pytestmark = [pytest.mark.unit, pytest.mark.task]


def escape_resp(resp):
    return re.sub("[\x00-\x20]+", " ", resp.strip())


@patch("backend.lambdas.tasks.execute_query.client")
@patch("backend.lambdas.tasks.execute_query.make_query")
def test_it_executes_queries(query_mock, client_mock):
    client_mock.start_query_execution.return_value = {"QueryExecutionId": "123"}
    query_mock.return_value = "test"

    resp = handler(
        {"QueryData": {}, "Bucket": "mybucket", "Prefix": "my_prefix"},
        SimpleNamespace(),
    )
    assert "123" == resp
    client_mock.start_query_execution.assert_called_with(
        QueryString="test",
        ResultConfiguration={"OutputLocation": "s3://mybucket/my_prefix/"},
        WorkGroup="primary",
    )


@patch("backend.lambdas.tasks.execute_query.client")
@patch("backend.lambdas.tasks.execute_query.make_query")
def test_it_permits_custom_workgroups(query_mock, client_mock):
    client_mock.start_query_execution.return_value = {"QueryExecutionId": "123"}
    query_mock.return_value = "test"
    with patch.dict(os.environ, {"WorkGroup": "custom"}):
        resp = handler(
            {"QueryData": {}, "Bucket": "mybucket", "Prefix": "my_prefix"},
            SimpleNamespace(),
        )
    assert "123" == resp
    client_mock.start_query_execution.assert_called_with(
        QueryString="test",
        ResultConfiguration={"OutputLocation": "s3://mybucket/my_prefix/"},
        WorkGroup="custom",
    )


def test_it_generates_query_with_partition():
    resp = make_query(
        {
            "Database": "amazonreviews",
            "Table": "amazon_reviews_parquet",
            "Columns": [{"Column": "customer_id", "Type": "Simple",}],
            "PartitionKeys": [{"Key": "product_category", "Value": "Books"}],
            "DataMapperId": "dm_1234",
            "JobId": "job_1234567890",
        }
    )
    assert escape_resp(resp) == escape_resp(
        """
            SELECT DISTINCT "$path" FROM (
                SELECT t."$path"
                FROM "amazonreviews"."amazon_reviews_parquet" t,
                    "s3f2_manifests_database"."s3f2_manifests_table" m
                WHERE
                    m."jobid"='job_1234567890' AND
                    m."datamapperid"='dm_1234' AND
                    cast(t."customer_id" as varchar)=m."queryablematchid" AND m."queryablecolumns"='customer_id'
                    AND "product_category" = 'Books'
            )
        """
    )


def test_it_generates_query_with_int_partition():
    resp = make_query(
        {
            "Database": "amazonreviews",
            "Table": "amazon_reviews_parquet",
            "Columns": [{"Column": "customer_id", "Type": "Simple",}],
            "PartitionKeys": [{"Key": "year", "Value": 2010}],
            "DataMapperId": "dm_1234",
            "JobId": "job_1234567890",
        }
    )
    assert escape_resp(resp) == escape_resp(
        """
            SELECT DISTINCT "$path" FROM (
                SELECT t."$path"
                FROM "amazonreviews"."amazon_reviews_parquet" t,
                    "s3f2_manifests_database"."s3f2_manifests_table" m
                WHERE
                    m."jobid"='job_1234567890' AND
                    m."datamapperid"='dm_1234' AND
                    cast(t."customer_id" as varchar)=m."queryablematchid" AND m."queryablecolumns"='customer_id'
                    AND "year" = 2010
            )
        """
    )


def test_it_generates_query_with_multiple_partitions():
    resp = make_query(
        {
            "Database": "amazonreviews",
            "Table": "amazon_reviews_parquet",
            "Columns": [{"Column": "customer_id", "Type": "Simple",}],
            "PartitionKeys": [
                {"Key": "product_category", "Value": "Books"},
                {"Key": "published", "Value": "2019"},
            ],
            "DataMapperId": "dm_1234",
            "JobId": "job_1234567890",
        }
    )
    assert escape_resp(resp) == escape_resp(
        """
            SELECT DISTINCT "$path" FROM (
                SELECT t."$path"
                FROM "amazonreviews"."amazon_reviews_parquet" t,
                    "s3f2_manifests_database"."s3f2_manifests_table" m
                WHERE
                    m."jobid"='job_1234567890' AND
                    m."datamapperid"='dm_1234' AND
                    cast(t."customer_id" as varchar)=m."queryablematchid" AND m."queryablecolumns"='customer_id'
                    AND "product_category" = 'Books'  AND "published" = '2019'
            )
        """
    )


def test_it_generates_query_without_partition():
    resp = make_query(
        {
            "Database": "amazonreviews",
            "Table": "amazon_reviews_parquet",
            "Columns": [{"Column": "customer_id", "Type": "Simple",}],
            "DataMapperId": "dm_1234",
            "JobId": "job_1234567890",
        }
    )
    assert escape_resp(resp) == escape_resp(
        """
            SELECT DISTINCT "$path" FROM (
                SELECT t."$path"
                FROM "amazonreviews"."amazon_reviews_parquet" t,
                    "s3f2_manifests_database"."s3f2_manifests_table" m
                WHERE
                    m."jobid"='job_1234567890' AND
                    m."datamapperid"='dm_1234' AND
                    cast(t."customer_id" as varchar)=m."queryablematchid" AND m."queryablecolumns"='customer_id'
            )
        """
    )


def test_it_generates_query_with_multiple_columns():
    resp = make_query(
        {
            "Database": "amazonreviews",
            "Table": "amazon_reviews_parquet",
            "Columns": [
                {"Column": "a", "Type": "Simple"},
                {"Column": "b", "Type": "Simple"},
            ],
            "DataMapperId": "dm_1234",
            "JobId": "job_1234567890",
        }
    )
    assert escape_resp(resp) == escape_resp(
        """
            SELECT DISTINCT "$path" FROM (
                SELECT t."$path"
                FROM "amazonreviews"."amazon_reviews_parquet" t,
                    "s3f2_manifests_database"."s3f2_manifests_table" m
                WHERE
                    m."jobid"='job_1234567890' AND
                    m."datamapperid"='dm_1234' AND
                    cast(t."a" as varchar)=m."queryablematchid" AND m."queryablecolumns"='a'

                UNION ALL

                SELECT t."$path"
                FROM "amazonreviews"."amazon_reviews_parquet" t,
                    "s3f2_manifests_database"."s3f2_manifests_table" m
                WHERE
                    m."jobid"='job_1234567890' AND
                    m."datamapperid"='dm_1234' AND
                    cast(t."b" as varchar)=m."queryablematchid" AND m."queryablecolumns"='b'
            )
        """
    )


def test_it_generates_query_with_columns_of_complex_type():
    resp = make_query(
        {
            "Database": "amazonreviews",
            "Table": "amazon_reviews_parquet",
            "Columns": [{"Column": "a.b.c", "Type": "Simple"}],
            "DataMapperId": "dm_1234",
            "JobId": "job_1234567890",
        }
    )
    assert escape_resp(resp) == escape_resp(
        """
            SELECT DISTINCT "$path" FROM (
                SELECT t."$path"
                FROM "amazonreviews"."amazon_reviews_parquet" t,
                    "s3f2_manifests_database"."s3f2_manifests_table" m
                WHERE
                    m."jobid"='job_1234567890' AND
                    m."datamapperid"='dm_1234' AND
                    cast(t."a"."b"."c" as varchar)=m."queryablematchid" AND m."queryablecolumns"='a.b.c'
            )
        """
    )


def test_it_generates_query_with_composite_matches():
    resp = make_query(
        {
            "Database": "amazonreviews",
            "Table": "amazon_reviews_parquet",
            "Columns": [
                {
                    "Columns": ["user.first_name", "user.last_name"],
                    "Type": "Composite",
                },
                {"Columns": ["user.age", "user.last_name"], "Type": "Composite",},
                {"Columns": ["user.userid"], "Type": "Composite",},
            ],
            "DataMapperId": "dm_1234",
            "JobId": "job_1234567890",
        }
    )
    assert escape_resp(resp) == escape_resp(
        """
            SELECT DISTINCT "$path" FROM (
                SELECT t."$path"
                FROM "amazonreviews"."amazon_reviews_parquet" t,
                    "s3f2_manifests_database"."s3f2_manifests_table" m
                WHERE
                    m."jobid"='job_1234567890' AND
                    m."datamapperid"='dm_1234' AND
                    concat(t."user"."first_name", '_S3F2COMP_', t."user"."last_name")=m."queryablematchid" AND
                    m."queryablecolumns"='user.first_name_S3F2COMP_user.last_name'

                UNION ALL

                SELECT t."$path"
                FROM "amazonreviews"."amazon_reviews_parquet" t,
                    "s3f2_manifests_database"."s3f2_manifests_table" m
                WHERE
                    m."jobid"='job_1234567890' AND
                    m."datamapperid"='dm_1234' AND
                    concat(t."user"."age", '_S3F2COMP_', t."user"."last_name")=m."queryablematchid" AND
                    m."queryablecolumns"='user.age_S3F2COMP_user.last_name'

                UNION ALL

                SELECT t."$path"
                FROM "amazonreviews"."amazon_reviews_parquet" t,
                    "s3f2_manifests_database"."s3f2_manifests_table" m
                WHERE
                    m."jobid"='job_1234567890' AND
                    m."datamapperid"='dm_1234' AND
                    cast(t."user"."userid" as varchar)=m."queryablematchid" AND m."queryablecolumns"='user.userid'
            )
        """
    )


def test_it_generates_query_with_simple_and_composite_matches():
    resp = make_query(
        {
            "Database": "amazonreviews",
            "Table": "amazon_reviews_parquet",
            "Columns": [
                {"Column": "a.b.c", "Type": "Simple",},
                {
                    "Columns": ["user.first_name", "user.last_name"],
                    "Type": "Composite",
                },
            ],
            "DataMapperId": "dm_1234",
            "JobId": "job_1234567890",
        },
    )
    assert escape_resp(resp) == escape_resp(
        """
            SELECT DISTINCT "$path" FROM (
                SELECT t."$path"
                FROM "amazonreviews"."amazon_reviews_parquet" t,
                    "s3f2_manifests_database"."s3f2_manifests_table" m
                WHERE
                    m."jobid"='job_1234567890' AND
                    m."datamapperid"='dm_1234' AND
                    cast(t."a"."b"."c" as varchar)=m."queryablematchid" AND m."queryablecolumns"='a.b.c'

                UNION ALL

                SELECT t."$path"
                FROM "amazonreviews"."amazon_reviews_parquet" t,
                    "s3f2_manifests_database"."s3f2_manifests_table" m
                WHERE
                    m."jobid"='job_1234567890' AND
                    m."datamapperid"='dm_1234' AND
                    concat(t."user"."first_name", '_S3F2COMP_', t."user"."last_name")=m."queryablematchid" AND
                    m."queryablecolumns"='user.first_name_S3F2COMP_user.last_name'
            )
        """
    )


def test_it_escapes_strings():
    assert "''' OR 1=1'" == escape_item("' OR 1=1")


def test_it_escapes_ints():
    assert 2 == escape_item(2)


def test_it_escapes_floats():
    assert float(2) == escape_item(float(2))


def test_it_escapes_none():
    assert "NULL" == escape_item(None)


def test_it_raises_for_unsupported_type():
    with pytest.raises(ValueError):
        escape_item(["val"])

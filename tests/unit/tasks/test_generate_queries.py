import json
import os
from types import SimpleNamespace

import mock
import pytest
from mock import patch, MagicMock

with patch.dict(os.environ, {"QueryQueue": "test"}):
    from backend.lambdas.tasks.generate_queries import (
        cast_to_type,
        generate_athena_queries,
        get_data_mappers,
        get_deletion_queue,
        get_inner_children,
        get_nested_children,
        get_partitions,
        get_table,
        handler,
        write_partitions,
    )

pytestmark = [pytest.mark.unit, pytest.mark.task]


def lists_equal_ignoring_order(a, b):
    a = a.copy()
    try:
        for item in b:
            a.remove(item)
    except ValueError:
        return False
    return not a


@patch("backend.lambdas.tasks.generate_queries.write_partitions")
@patch("backend.lambdas.tasks.generate_queries.batch_sqs_msgs")
@patch("backend.lambdas.tasks.generate_queries.get_deletion_queue")
@patch("backend.lambdas.tasks.generate_queries.get_data_mappers")
@patch("backend.lambdas.tasks.generate_queries.generate_athena_queries")
def test_it_generates_queries_writes_manifests_populates_queue_and_returns_result(
    gen_athena_queries,
    get_data_mappers,
    get_del_q,
    batch_sqs_msgs_mock,
    write_partitions_mock,
):
    queue = [{"MatchId": "hi", "DeletionQueueItemId": "id123"}]
    queries = [
        {
            "DataMapperId": "a",
            "QueryExecutor": "athena",
            "Format": "parquet",
            "Database": "test_db",
            "Table": "test_table",
            "Columns": [{"Column": "customer_id"}],
            "PartitionKeys": [{"Key": "product_category", "Value": "Books"}],
            "DeleteOldVersions": True,
            "IgnoreObjectNotFoundExceptions": False,
            "Manifest": "s3://S3F2-manifests-bucket/manifests/test/a/manifest.json",
        }
    ]
    data_mapper = {
        "DataMapperId": "a",
        "QueryExecutor": "athena",
        "Columns": ["customer_id"],
        "Format": "parquet",
        "QueryExecutorParameters": {
            "DataCatalogProvider": "glue",
            "Database": "test_db",
            "Table": "test_table",
        },
    }

    get_del_q.return_value = queue
    gen_athena_queries.return_value = queries
    get_data_mappers.return_value = iter([data_mapper])
    result = handler({"ExecutionName": "test"}, SimpleNamespace())

    gen_athena_queries.assert_called_with(data_mapper, queue, "test")
    write_partitions_mock.assert_called_with([["test", "a"]])
    batch_sqs_msgs_mock.assert_called_with(mock.ANY, queries)
    assert result == {
        "GeneratedQueries": 1,
        "DeletionQueueSize": 1,
        "Manifests": ["s3://S3F2-manifests-bucket/manifests/test/a/manifest.json"],
    }


@patch("backend.lambdas.tasks.generate_queries.batch_sqs_msgs")
@patch("backend.lambdas.tasks.generate_queries.get_deletion_queue")
@patch("backend.lambdas.tasks.generate_queries.get_data_mappers")
def test_it_raises_for_unknown_query_executor(
    get_data_mappers, get_del_q, batch_sqs_msgs_mock
):
    get_del_q.return_value = [{"MatchId": "hi"}]
    get_data_mappers.return_value = iter(
        [
            {
                "DataMapperId": "a",
                "QueryExecutor": "invalid",
                "Columns": ["customer_id"],
                "Format": "parquet",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test_db",
                    "Table": "test_table",
                },
            }
        ]
    )
    with pytest.raises(NotImplementedError):
        handler({"ExecutionName": "test"}, SimpleNamespace())
        batch_sqs_msgs_mock.assert_not_called()


class TestAthenaQueries:
    @patch("backend.lambdas.tasks.generate_queries.s3.Bucket")
    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_handles_single_columns(
        self, get_partitions_mock, get_table_mock, bucket_mock
    ):
        put_object_mock = MagicMock()
        bucket_mock.return_value = put_object_mock
        columns = [{"Name": "customer_id"}]
        partition_keys = ["product_category"]
        partitions = [["Books"]]
        get_table_mock.return_value = table_stub(columns, partition_keys)
        get_partitions_mock.return_value = [
            partition_stub(p, columns) for p in partitions
        ]
        resp = generate_athena_queries(
            {
                "DataMapperId": "a",
                "QueryExecutor": "athena",
                "Columns": [col["Name"] for col in columns],
                "Format": "parquet",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test_db",
                    "Table": "test_table",
                },
            },
            [
                {
                    "MatchId": "hi",
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "id-01",
                }
            ],
            "job_1234567890",
        )
        assert resp == [
            {
                "DataMapperId": "a",
                "QueryExecutor": "athena",
                "Format": "parquet",
                "Database": "test_db",
                "Table": "test_table",
                "Columns": [{"Column": "customer_id", "Type": "Simple"}],
                "PartitionKeys": [{"Key": "product_category", "Value": "Books"}],
                "DeleteOldVersions": True,
                "IgnoreObjectNotFoundExceptions": False,
                "Manifest": "s3://S3F2-manifests-bucket/manifests/job_1234567890/a/manifest.json",
            }
        ]
        put_object_mock.put_object.assert_called_with(
            Key="manifests/job_1234567890/a/manifest.json",
            Body=json.dumps(
                {
                    "Columns": ["customer_id"],
                    "MatchId": ["hi"],
                    "DeletionQueueItemId": "id-01",
                    "CreatedAt": 1614698440,
                    "QueryableColumns": "customer_id",
                    "QueryableMatchId": "hi",
                }
            )
            + "\n",
        )

    @patch("backend.lambdas.tasks.generate_queries.s3.Bucket")
    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_handles_int_matches(
        self, get_partitions_mock, get_table_mock, bucket_mock
    ):
        put_object_mock = MagicMock()
        bucket_mock.return_value = put_object_mock
        columns = [{"Name": "customer_id", "Type": "int"}]
        partition_keys = ["product_category"]
        partitions = [["Books"]]
        get_table_mock.return_value = table_stub(columns, partition_keys)
        get_partitions_mock.return_value = [
            partition_stub(p, columns) for p in partitions
        ]
        resp = generate_athena_queries(
            {
                "DataMapperId": "a",
                "QueryExecutor": "athena",
                "Columns": [col["Name"] for col in columns],
                "Format": "parquet",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test_db",
                    "Table": "test_table",
                },
            },
            [
                {
                    "MatchId": 12345,
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "id-01",
                },
                {
                    "MatchId": 23456,
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "id-02",
                },
            ],
            "job_1234567890",
        )
        assert resp == [
            {
                "DataMapperId": "a",
                "QueryExecutor": "athena",
                "Format": "parquet",
                "Database": "test_db",
                "Table": "test_table",
                "Columns": [{"Column": "customer_id", "Type": "Simple",}],
                "PartitionKeys": [{"Key": "product_category", "Value": "Books"}],
                "DeleteOldVersions": True,
                "IgnoreObjectNotFoundExceptions": False,
                "Manifest": "s3://S3F2-manifests-bucket/manifests/job_1234567890/a/manifest.json",
            }
        ]
        put_object_mock.put_object.assert_called_with(
            Key="manifests/job_1234567890/a/manifest.json",
            Body=(
                json.dumps(
                    {
                        "Columns": ["customer_id"],
                        "MatchId": [12345],
                        "DeletionQueueItemId": "id-01",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "customer_id",
                        "QueryableMatchId": "12345",
                    }
                )
                + "\n"
                + json.dumps(
                    {
                        "Columns": ["customer_id"],
                        "MatchId": [23456],
                        "DeletionQueueItemId": "id-02",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "customer_id",
                        "QueryableMatchId": "23456",
                    }
                )
                + "\n"
            ),
        )

    @patch("backend.lambdas.tasks.generate_queries.s3.Bucket")
    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_handles_decimal_matches(
        self, get_partitions_mock, get_table_mock, bucket_mock
    ):
        put_object_mock = MagicMock()
        bucket_mock.return_value = put_object_mock
        columns = [{"Name": "customer_id", "Type": "decimal"}]
        partition_keys = ["product_category"]
        partitions = [["Books"]]
        get_table_mock.return_value = table_stub(columns, partition_keys)
        get_partitions_mock.return_value = [
            partition_stub(p, columns) for p in partitions
        ]
        resp = generate_athena_queries(
            {
                "DataMapperId": "a",
                "QueryExecutor": "athena",
                "Columns": [col["Name"] for col in columns],
                "Format": "parquet",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test_db",
                    "Table": "test_table",
                },
            },
            [
                {
                    "MatchId": "12.30",
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "id-01",
                },
                {
                    "MatchId": "23.400",
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "id-02",
                },
            ],
            "job_1234567890",
        )
        assert resp == [
            {
                "DataMapperId": "a",
                "QueryExecutor": "athena",
                "Format": "parquet",
                "Database": "test_db",
                "Table": "test_table",
                "Columns": [{"Column": "customer_id", "Type": "Simple",}],
                "PartitionKeys": [{"Key": "product_category", "Value": "Books"}],
                "DeleteOldVersions": True,
                "IgnoreObjectNotFoundExceptions": False,
                "Manifest": "s3://S3F2-manifests-bucket/manifests/job_1234567890/a/manifest.json",
            }
        ]
        put_object_mock.put_object.assert_called_with(
            Key="manifests/job_1234567890/a/manifest.json",
            Body=(
                json.dumps(
                    {
                        "Columns": ["customer_id"],
                        "MatchId": ["12.30"],
                        "DeletionQueueItemId": "id-01",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "customer_id",
                        "QueryableMatchId": "12.30",
                    }
                )
                + "\n"
                + json.dumps(
                    {
                        "Columns": ["customer_id"],
                        "MatchId": ["23.400"],
                        "DeletionQueueItemId": "id-02",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "customer_id",
                        "QueryableMatchId": "23.400",
                    }
                )
                + "\n"
            ),
        )

    @patch("backend.lambdas.tasks.generate_queries.s3.Bucket")
    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_handles_int_partitions(
        self, get_partitions_mock, get_table_mock, bucket_mock
    ):
        put_object_mock = MagicMock()
        bucket_mock.return_value = put_object_mock
        columns = [{"Name": "customer_id"}]
        partition_keys = ["year"]
        partitions = [["2010"]]
        get_table_mock.return_value = table_stub(
            columns, partition_keys, partition_keys_type="int"
        )
        get_partitions_mock.return_value = [
            partition_stub(p, columns) for p in partitions
        ]
        resp = generate_athena_queries(
            {
                "DataMapperId": "a",
                "QueryExecutor": "athena",
                "Columns": [col["Name"] for col in columns],
                "Format": "parquet",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test_db",
                    "Table": "test_table",
                },
            },
            [
                {
                    "MatchId": "hi",
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "id-01",
                }
            ],
            "job_1234567890",
        )
        assert resp == [
            {
                "DataMapperId": "a",
                "QueryExecutor": "athena",
                "Format": "parquet",
                "Database": "test_db",
                "Table": "test_table",
                "Columns": [{"Column": "customer_id", "Type": "Simple"}],
                "PartitionKeys": [{"Key": "year", "Value": 2010}],
                "DeleteOldVersions": True,
                "IgnoreObjectNotFoundExceptions": False,
                "Manifest": "s3://S3F2-manifests-bucket/manifests/job_1234567890/a/manifest.json",
            }
        ]
        put_object_mock.put_object.assert_called_with(
            Key="manifests/job_1234567890/a/manifest.json",
            Body=json.dumps(
                {
                    "Columns": ["customer_id"],
                    "MatchId": ["hi"],
                    "DeletionQueueItemId": "id-01",
                    "CreatedAt": 1614698440,
                    "QueryableColumns": "customer_id",
                    "QueryableMatchId": "hi",
                }
            )
            + "\n",
        )

    @patch("backend.lambdas.tasks.generate_queries.s3.Bucket")
    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_handles_multiple_columns(
        self, get_partitions_mock, get_table_mock, bucket_mock
    ):
        put_object_mock = MagicMock()
        bucket_mock.return_value = put_object_mock
        columns = [{"Name": "customer_id"}, {"Name": "alt_customer_id"}]
        partition_keys = ["product_category"]
        partitions = [["Books"]]
        get_table_mock.return_value = table_stub(columns, partition_keys)
        get_partitions_mock.return_value = [
            partition_stub(p, columns) for p in partitions
        ]
        resp = generate_athena_queries(
            {
                "DataMapperId": "a",
                "QueryExecutor": "athena",
                "Columns": [col["Name"] for col in columns],
                "Format": "parquet",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test_db",
                    "Table": "test_table",
                },
            },
            [
                {
                    "MatchId": "hi",
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "id-01",
                }
            ],
            "job_1234567890",
        )

        assert resp == [
            {
                "DataMapperId": "a",
                "QueryExecutor": "athena",
                "Format": "parquet",
                "Database": "test_db",
                "Table": "test_table",
                "Columns": [
                    {"Column": "customer_id", "Type": "Simple"},
                    {"Column": "alt_customer_id", "Type": "Simple"},
                ],
                "PartitionKeys": [{"Key": "product_category", "Value": "Books"}],
                "DeleteOldVersions": True,
                "IgnoreObjectNotFoundExceptions": False,
                "Manifest": "s3://S3F2-manifests-bucket/manifests/job_1234567890/a/manifest.json",
            }
        ]
        put_object_mock.put_object.assert_called_with(
            Key="manifests/job_1234567890/a/manifest.json",
            Body=(
                json.dumps(
                    {
                        "Columns": ["customer_id"],
                        "MatchId": ["hi"],
                        "DeletionQueueItemId": "id-01",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "customer_id",
                        "QueryableMatchId": "hi",
                    }
                )
                + "\n"
                + json.dumps(
                    {
                        "Columns": ["alt_customer_id"],
                        "MatchId": ["hi"],
                        "DeletionQueueItemId": "id-01",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "alt_customer_id",
                        "QueryableMatchId": "hi",
                    }
                )
                + "\n"
            ),
        )

    @patch("backend.lambdas.tasks.generate_queries.s3.Bucket")
    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_handles_composite_columns(
        self, get_partitions_mock, get_table_mock, bucket_mock
    ):
        put_object_mock = MagicMock()
        bucket_mock.return_value = put_object_mock
        columns = [
            {"Name": "first_name"},
            {"Name": "last_name"},
        ]
        partition_keys = ["product_category"]
        partitions = [["Books"]]
        get_table_mock.return_value = table_stub(columns, partition_keys)
        get_partitions_mock.return_value = [
            partition_stub(p, columns) for p in partitions
        ]
        resp = generate_athena_queries(
            {
                "DataMapperId": "a",
                "QueryExecutor": "athena",
                "Columns": [col["Name"] for col in columns],
                "Format": "parquet",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test_db",
                    "Table": "test_table",
                },
            },
            [
                {
                    "MatchId": [
                        {"Column": "first_name", "Value": "John"},
                        {"Column": "last_name", "Value": "Doe"},
                    ],
                    "Type": "Composite",
                    "DataMappers": ["a"],
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "id1234",
                }
            ],
            "job_1234567890",
        )

        assert resp == [
            {
                "DataMapperId": "a",
                "QueryExecutor": "athena",
                "Format": "parquet",
                "Database": "test_db",
                "Table": "test_table",
                "Columns": [
                    {"Columns": ["first_name", "last_name"], "Type": "Composite",}
                ],
                "PartitionKeys": [{"Key": "product_category", "Value": "Books"}],
                "DeleteOldVersions": True,
                "IgnoreObjectNotFoundExceptions": False,
                "Manifest": "s3://S3F2-manifests-bucket/manifests/job_1234567890/a/manifest.json",
            }
        ]
        put_object_mock.put_object.assert_called_with(
            Key="manifests/job_1234567890/a/manifest.json",
            Body=(
                json.dumps(
                    {
                        "Columns": ["first_name", "last_name"],
                        "MatchId": ["John", "Doe"],
                        "DeletionQueueItemId": "id1234",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "first_name_S3F2COMP_last_name",
                        "QueryableMatchId": "John_S3F2COMP_Doe",
                    }
                )
                + "\n"
            ),
        )

    @patch("backend.lambdas.tasks.generate_queries.s3.Bucket")
    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_handles_mixed_columns(
        self, get_partitions_mock, get_table_mock, bucket_mock
    ):
        put_object_mock = MagicMock()
        bucket_mock.return_value = put_object_mock
        columns = [
            {"Name": "customer_id"},
            {"Name": "first_name"},
            {"Name": "last_name"},
            {"Name": "age", "Type": "int"},
        ]
        partition_keys = ["product_category"]
        partitions = [["Books"]]
        get_table_mock.return_value = table_stub(columns, partition_keys)
        get_partitions_mock.return_value = [
            partition_stub(p, columns) for p in partitions
        ]
        resp = generate_athena_queries(
            {
                "DataMapperId": "a",
                "QueryExecutor": "athena",
                "Columns": [col["Name"] for col in columns],
                "Format": "parquet",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test_db",
                    "Table": "test_table",
                },
            },
            [
                {
                    "MatchId": "12345",
                    "Type": "Simple",
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "id001",
                },
                {
                    "MatchId": "23456",
                    "Type": "Simple",
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "id002",
                },
                {
                    "MatchId": "23456",
                    "Type": "Simple",
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "id003",
                },
                {
                    "MatchId": [
                        {"Column": "first_name", "Value": "John"},
                        {"Column": "last_name", "Value": "Doe"},
                    ],
                    "Type": "Composite",
                    "DataMappers": ["a"],
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "id004",
                },
                {
                    "MatchId": [
                        {"Column": "first_name", "Value": "Jane"},
                        {"Column": "last_name", "Value": "Doe"},
                    ],
                    "Type": "Composite",
                    "DataMappers": ["a"],
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "id005",
                },
                {
                    "MatchId": [
                        {"Column": "first_name", "Value": "Jane"},
                        {"Column": "last_name", "Value": "Doe"},
                    ],
                    "Type": "Composite",
                    "DataMappers": ["a"],
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "id006",
                },
                {
                    "MatchId": [
                        {"Column": "last_name", "Value": "Smith"},
                        {"Column": "age", "Value": "28"},
                    ],
                    "Type": "Composite",
                    "DataMappers": ["a"],
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "id007",
                },
            ],
            "job1234567890",
        )

        assert resp == [
            {
                "DataMapperId": "a",
                "QueryExecutor": "athena",
                "Format": "parquet",
                "Database": "test_db",
                "Table": "test_table",
                "Columns": [
                    {"Column": "customer_id", "Type": "Simple",},
                    {"Column": "first_name", "Type": "Simple",},
                    {"Column": "last_name", "Type": "Simple",},
                    {"Column": "age", "Type": "Simple"},
                    {"Columns": ["first_name", "last_name"], "Type": "Composite",},
                    {"Columns": ["age", "last_name"], "Type": "Composite",},
                ],
                "PartitionKeys": [{"Key": "product_category", "Value": "Books"}],
                "DeleteOldVersions": True,
                "IgnoreObjectNotFoundExceptions": False,
                "Manifest": "s3://S3F2-manifests-bucket/manifests/job1234567890/a/manifest.json",
            }
        ]
        put_object_mock.put_object.assert_called_with(
            Key="manifests/job1234567890/a/manifest.json",
            Body=(
                # id001 simple on all columns
                json.dumps(
                    {
                        "Columns": ["customer_id"],
                        "MatchId": ["12345"],
                        "DeletionQueueItemId": "id001",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "customer_id",
                        "QueryableMatchId": "12345",
                    }
                )
                + "\n"
                + json.dumps(
                    {
                        "Columns": ["first_name"],
                        "MatchId": ["12345"],
                        "DeletionQueueItemId": "id001",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "first_name",
                        "QueryableMatchId": "12345",
                    }
                )
                + "\n"
                + json.dumps(
                    {
                        "Columns": ["last_name"],
                        "MatchId": ["12345"],
                        "DeletionQueueItemId": "id001",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "last_name",
                        "QueryableMatchId": "12345",
                    }
                )
                + "\n"
                + json.dumps(
                    {
                        "Columns": ["age"],
                        "MatchId": [12345],
                        "DeletionQueueItemId": "id001",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "age",
                        "QueryableMatchId": "12345",
                    }
                )
                + "\n"
                # id002 simple on all columns
                + json.dumps(
                    {
                        "Columns": ["customer_id"],
                        "MatchId": ["23456"],
                        "DeletionQueueItemId": "id002",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "customer_id",
                        "QueryableMatchId": "23456",
                    }
                )
                + "\n"
                + json.dumps(
                    {
                        "Columns": ["first_name"],
                        "MatchId": ["23456"],
                        "DeletionQueueItemId": "id002",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "first_name",
                        "QueryableMatchId": "23456",
                    }
                )
                + "\n"
                + json.dumps(
                    {
                        "Columns": ["last_name"],
                        "MatchId": ["23456"],
                        "DeletionQueueItemId": "id002",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "last_name",
                        "QueryableMatchId": "23456",
                    }
                )
                + "\n"
                + json.dumps(
                    {
                        "Columns": ["age"],
                        "MatchId": [23456],
                        "DeletionQueueItemId": "id002",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "age",
                        "QueryableMatchId": "23456",
                    }
                )
                + "\n"
                # id003 is a id002 clone
                # Values are same as id002 but we cannot deduplicate
                # as we need id003 too for the cleanup phase
                + json.dumps(
                    {
                        "Columns": ["customer_id"],
                        "MatchId": ["23456"],
                        "DeletionQueueItemId": "id003",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "customer_id",
                        "QueryableMatchId": "23456",
                    }
                )
                + "\n"
                + json.dumps(
                    {
                        "Columns": ["first_name"],
                        "MatchId": ["23456"],
                        "DeletionQueueItemId": "id003",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "first_name",
                        "QueryableMatchId": "23456",
                    }
                )
                + "\n"
                + json.dumps(
                    {
                        "Columns": ["last_name"],
                        "MatchId": ["23456"],
                        "DeletionQueueItemId": "id003",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "last_name",
                        "QueryableMatchId": "23456",
                    }
                )
                + "\n"
                + json.dumps(
                    {
                        "Columns": ["age"],
                        "MatchId": [23456],
                        "DeletionQueueItemId": "id003",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "age",
                        "QueryableMatchId": "23456",
                    }
                )
                + "\n"
                # id004 composite multi-column
                + json.dumps(
                    {
                        "Columns": ["first_name", "last_name"],
                        "MatchId": ["John", "Doe"],
                        "DeletionQueueItemId": "id004",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "first_name_S3F2COMP_last_name",
                        "QueryableMatchId": "John_S3F2COMP_Doe",
                    }
                )
                + "\n"
                # id005 composite multi-column
                + json.dumps(
                    {
                        "Columns": ["first_name", "last_name"],
                        "MatchId": ["Jane", "Doe"],
                        "DeletionQueueItemId": "id005",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "first_name_S3F2COMP_last_name",
                        "QueryableMatchId": "Jane_S3F2COMP_Doe",
                    }
                )
                + "\n"
                # id006 is a id005 clone
                + json.dumps(
                    {
                        "Columns": ["first_name", "last_name"],
                        "MatchId": ["Jane", "Doe"],
                        "DeletionQueueItemId": "id006",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "first_name_S3F2COMP_last_name",
                        "QueryableMatchId": "Jane_S3F2COMP_Doe",
                    }
                )
                + "\n"
                # id007 composite multi-column with different types
                # note that columns are sorted alphabetically
                + json.dumps(
                    {
                        "Columns": ["age", "last_name"],
                        "MatchId": [28, "Smith"],
                        "DeletionQueueItemId": "id007",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "age_S3F2COMP_last_name",
                        "QueryableMatchId": "28_S3F2COMP_Smith",
                    }
                )
                + "\n"
            ),
        )

    @patch("backend.lambdas.tasks.generate_queries.s3.Bucket")
    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_handles_multiple_partition_keys(
        self, get_partitions_mock, get_table_mock, bucket_mock
    ):
        put_object_mock = MagicMock()
        bucket_mock.return_value = put_object_mock
        columns = [{"Name": "customer_id"}]
        partition_keys = ["year", "month"]
        partitions = [["2019", "01"]]
        get_table_mock.return_value = table_stub(columns, partition_keys)
        get_partitions_mock.return_value = [
            partition_stub(p, columns) for p in partitions
        ]

        resp = generate_athena_queries(
            {
                "DataMapperId": "a",
                "QueryExecutor": "athena",
                "Columns": [col["Name"] for col in columns],
                "Format": "parquet",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test_db",
                    "Table": "test_table",
                },
            },
            [
                {
                    "MatchId": "hi",
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "id1234",
                }
            ],
            "job_1234567890",
        )

        assert resp == [
            {
                "DataMapperId": "a",
                "QueryExecutor": "athena",
                "Format": "parquet",
                "Database": "test_db",
                "Table": "test_table",
                "Columns": [{"Column": "customer_id", "Type": "Simple"}],
                "PartitionKeys": [
                    {"Key": "year", "Value": "2019"},
                    {"Key": "month", "Value": "01"},
                ],
                "DeleteOldVersions": True,
                "IgnoreObjectNotFoundExceptions": False,
                "Manifest": "s3://S3F2-manifests-bucket/manifests/job_1234567890/a/manifest.json",
            }
        ]
        put_object_mock.put_object.assert_called_with(
            Key="manifests/job_1234567890/a/manifest.json",
            Body=(
                json.dumps(
                    {
                        "Columns": ["customer_id"],
                        "MatchId": ["hi"],
                        "DeletionQueueItemId": "id1234",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "customer_id",
                        "QueryableMatchId": "hi",
                    }
                )
                + "\n"
            ),
        )

    @patch("backend.lambdas.tasks.generate_queries.s3.Bucket")
    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_handles_multiple_partition_values(
        self, get_partitions_mock, get_table_mock, bucket_mock
    ):
        put_object_mock = MagicMock()
        bucket_mock.return_value = put_object_mock
        columns = [{"Name": "customer_id"}]
        partition_keys = ["year", "month"]
        partitions = [["2018", "12"], ["2019", "01"], ["2019", "02"]]
        get_table_mock.return_value = table_stub(columns, partition_keys)
        get_partitions_mock.return_value = [
            partition_stub(p, columns) for p in partitions
        ]

        resp = generate_athena_queries(
            {
                "DataMapperId": "a",
                "Columns": [col["Name"] for col in columns],
                "Format": "parquet",
                "QueryExecutor": "athena",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test_db",
                    "Table": "test_table",
                },
            },
            [
                {
                    "MatchId": "hi",
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "item1234",
                }
            ],
            "job_1234567890",
        )

        assert lists_equal_ignoring_order(
            resp,
            [
                {
                    "DataMapperId": "a",
                    "Database": "test_db",
                    "Table": "test_table",
                    "QueryExecutor": "athena",
                    "Format": "parquet",
                    "Columns": [{"Column": "customer_id", "Type": "Simple"}],
                    "PartitionKeys": [
                        {"Key": "year", "Value": "2018"},
                        {"Key": "month", "Value": "12"},
                    ],
                    "DeleteOldVersions": True,
                    "IgnoreObjectNotFoundExceptions": False,
                    "Manifest": "s3://S3F2-manifests-bucket/manifests/job_1234567890/a/manifest.json",
                },
                {
                    "DataMapperId": "a",
                    "Database": "test_db",
                    "Table": "test_table",
                    "QueryExecutor": "athena",
                    "Format": "parquet",
                    "Columns": [{"Column": "customer_id", "Type": "Simple"}],
                    "PartitionKeys": [
                        {"Key": "year", "Value": "2019"},
                        {"Key": "month", "Value": "01"},
                    ],
                    "DeleteOldVersions": True,
                    "IgnoreObjectNotFoundExceptions": False,
                    "Manifest": "s3://S3F2-manifests-bucket/manifests/job_1234567890/a/manifest.json",
                },
                {
                    "DataMapperId": "a",
                    "Database": "test_db",
                    "Table": "test_table",
                    "QueryExecutor": "athena",
                    "Format": "parquet",
                    "Columns": [{"Column": "customer_id", "Type": "Simple"}],
                    "PartitionKeys": [
                        {"Key": "year", "Value": "2019"},
                        {"Key": "month", "Value": "02"},
                    ],
                    "DeleteOldVersions": True,
                    "IgnoreObjectNotFoundExceptions": False,
                    "Manifest": "s3://S3F2-manifests-bucket/manifests/job_1234567890/a/manifest.json",
                },
            ],
        )
        put_object_mock.put_object.assert_called_with(
            Key="manifests/job_1234567890/a/manifest.json",
            Body=(
                json.dumps(
                    {
                        "Columns": ["customer_id"],
                        "MatchId": ["hi"],
                        "DeletionQueueItemId": "item1234",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "customer_id",
                        "QueryableMatchId": "hi",
                    }
                )
                + "\n"
            ),
        )

    @patch("backend.lambdas.tasks.generate_queries.s3.Bucket")
    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_propagates_optional_properties(
        self, get_partitions_mock, get_table_mock, bucket_mock
    ):
        put_object_mock = MagicMock()
        bucket_mock.return_value = put_object_mock
        columns = [{"Name": "customer_id"}]
        partition_keys = ["year", "month"]
        partitions = [["2018", "12"], ["2019", "01"]]
        get_table_mock.return_value = table_stub(columns, partition_keys)
        get_partitions_mock.return_value = [
            partition_stub(p, columns) for p in partitions
        ]

        resp = generate_athena_queries(
            {
                "DataMapperId": "a",
                "QueryExecutor": "athena",
                "Columns": [col["Name"] for col in columns],
                "Format": "parquet",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test_db",
                    "Table": "test_table",
                },
                "RoleArn": "arn:aws:iam::accountid:role/rolename",
                "DeleteOldVersions": True,
                "IgnoreObjectNotFoundExceptions": True,
            },
            [
                {
                    "MatchId": "hi",
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "item1234",
                }
            ],
            "job_1234567890",
        )

        assert lists_equal_ignoring_order(
            resp,
            [
                {
                    "DataMapperId": "a",
                    "Database": "test_db",
                    "Table": "test_table",
                    "QueryExecutor": "athena",
                    "Format": "parquet",
                    "Columns": [{"Column": "customer_id", "Type": "Simple"}],
                    "PartitionKeys": [
                        {"Key": "year", "Value": "2018"},
                        {"Key": "month", "Value": "12"},
                    ],
                    "RoleArn": "arn:aws:iam::accountid:role/rolename",
                    "DeleteOldVersions": True,
                    "IgnoreObjectNotFoundExceptions": True,
                    "Manifest": "s3://S3F2-manifests-bucket/manifests/job_1234567890/a/manifest.json",
                },
                {
                    "DataMapperId": "a",
                    "Database": "test_db",
                    "Table": "test_table",
                    "QueryExecutor": "athena",
                    "Format": "parquet",
                    "Columns": [{"Column": "customer_id", "Type": "Simple"}],
                    "PartitionKeys": [
                        {"Key": "year", "Value": "2019"},
                        {"Key": "month", "Value": "01"},
                    ],
                    "RoleArn": "arn:aws:iam::accountid:role/rolename",
                    "DeleteOldVersions": True,
                    "IgnoreObjectNotFoundExceptions": True,
                    "Manifest": "s3://S3F2-manifests-bucket/manifests/job_1234567890/a/manifest.json",
                },
            ],
        )
        put_object_mock.put_object.assert_called_with(
            Key="manifests/job_1234567890/a/manifest.json",
            Body=(
                json.dumps(
                    {
                        "Columns": ["customer_id"],
                        "MatchId": ["hi"],
                        "DeletionQueueItemId": "item1234",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "customer_id",
                        "QueryableMatchId": "hi",
                    }
                )
                + "\n"
            ),
        )

    @patch("backend.lambdas.tasks.generate_queries.s3.Bucket")
    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_filters_users_from_non_applicable_tables(
        self, get_partitions_mock, get_table_mock, bucket_mock
    ):
        put_object_mock = MagicMock()
        bucket_mock.return_value = put_object_mock
        columns = [{"Name": "customer_id"}]
        partition_keys = ["product_category"]
        partitions = [["Books"]]
        get_table_mock.return_value = table_stub(columns, partition_keys)
        get_partitions_mock.return_value = [
            partition_stub(p, columns) for p in partitions
        ]
        resp = generate_athena_queries(
            {
                "DataMapperId": "B",
                "QueryExecutor": "athena",
                "Columns": [col["Name"] for col in columns],
                "Format": "parquet",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test_db",
                    "Table": "B",
                },
            },
            [
                {
                    "MatchId": "123",
                    "CreatedAt": 1614698440,
                    "DataMappers": ["A"],
                    "DeletionQueueItemId": "id1",
                },
                {
                    "MatchId": "456",
                    "CreatedAt": 1614698440,
                    "DataMappers": [],
                    "DeletionQueueItemId": "id2",
                },
            ],
            "job_1234567890",
        )

        assert resp == [
            {
                "DataMapperId": "B",
                "Database": "test_db",
                "Table": "B",
                "QueryExecutor": "athena",
                "Format": "parquet",
                "Columns": [{"Column": "customer_id", "Type": "Simple"}],
                "PartitionKeys": [{"Key": "product_category", "Value": "Books"}],
                "DeleteOldVersions": True,
                "IgnoreObjectNotFoundExceptions": False,
                "Manifest": "s3://S3F2-manifests-bucket/manifests/job_1234567890/B/manifest.json",
            }
        ]
        put_object_mock.put_object.assert_called_with(
            Key="manifests/job_1234567890/B/manifest.json",
            Body=(
                json.dumps(
                    {
                        "Columns": ["customer_id"],
                        "MatchId": ["456"],
                        "DeletionQueueItemId": "id2",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "customer_id",
                        "QueryableMatchId": "456",
                    }
                )
                + "\n"
            ),
        )

    @patch("backend.lambdas.tasks.generate_queries.s3.Bucket")
    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_handles_unpartitioned_data(
        self, get_partitions_mock, get_table_mock, bucket_mock
    ):
        put_object_mock = MagicMock()
        bucket_mock.return_value = put_object_mock
        columns = [{"Name": "customer_id"}]
        get_table_mock.return_value = table_stub(columns, [])
        get_partitions_mock.return_value = []
        resp = generate_athena_queries(
            {
                "DataMapperId": "a",
                "QueryExecutor": "athena",
                "Columns": [col["Name"] for col in columns],
                "Format": "parquet",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test_db",
                    "Table": "test_table",
                },
            },
            [
                {
                    "MatchId": "hi",
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "item1234",
                }
            ],
            "job_1234567890",
        )
        assert resp == [
            {
                "DataMapperId": "a",
                "Database": "test_db",
                "Table": "test_table",
                "QueryExecutor": "athena",
                "Format": "parquet",
                "Columns": [{"Column": "customer_id", "Type": "Simple"}],
                "PartitionKeys": [],
                "DeleteOldVersions": True,
                "IgnoreObjectNotFoundExceptions": False,
                "Manifest": "s3://S3F2-manifests-bucket/manifests/job_1234567890/a/manifest.json",
            }
        ]
        put_object_mock.put_object.assert_called_with(
            Key="manifests/job_1234567890/a/manifest.json",
            Body=(
                json.dumps(
                    {
                        "Columns": ["customer_id"],
                        "MatchId": ["hi"],
                        "DeletionQueueItemId": "item1234",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "customer_id",
                        "QueryableMatchId": "hi",
                    }
                )
                + "\n"
            ),
        )

    @patch("backend.lambdas.tasks.generate_queries.s3.Bucket")
    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_propagates_role_arn_for_unpartitioned_data(
        self, get_partitions_mock, get_table_mock, bucket_mock
    ):
        put_object_mock = MagicMock()
        bucket_mock.return_value = put_object_mock
        columns = [{"Name": "customer_id"}]
        get_table_mock.return_value = table_stub(columns, [])
        get_partitions_mock.return_value = []
        resp = generate_athena_queries(
            {
                "DataMapperId": "a",
                "QueryExecutor": "athena",
                "Columns": [col["Name"] for col in columns],
                "Format": "parquet",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test_db",
                    "Table": "test_table",
                },
                "RoleArn": "arn:aws:iam::accountid:role/rolename",
            },
            [
                {
                    "MatchId": "hi",
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "item1234",
                }
            ],
            "job_1234567890",
        )
        assert resp == [
            {
                "DataMapperId": "a",
                "Database": "test_db",
                "Table": "test_table",
                "QueryExecutor": "athena",
                "Format": "parquet",
                "Columns": [{"Column": "customer_id", "Type": "Simple"}],
                "PartitionKeys": [],
                "RoleArn": "arn:aws:iam::accountid:role/rolename",
                "DeleteOldVersions": True,
                "IgnoreObjectNotFoundExceptions": False,
                "Manifest": "s3://S3F2-manifests-bucket/manifests/job_1234567890/a/manifest.json",
            }
        ]
        put_object_mock.put_object.assert_called_with(
            Key="manifests/job_1234567890/a/manifest.json",
            Body=(
                json.dumps(
                    {
                        "Columns": ["customer_id"],
                        "MatchId": ["hi"],
                        "DeletionQueueItemId": "item1234",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "customer_id",
                        "QueryableMatchId": "hi",
                    }
                )
                + "\n"
            ),
        )

    @patch("backend.lambdas.tasks.generate_queries.s3.Bucket")
    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_removes_queries_with_no_applicable_matches(
        self, get_partitions_mock, get_table_mock, bucket_mock
    ):
        put_object_mock = MagicMock()
        bucket_mock.return_value = put_object_mock
        columns = [{"Name": "customer_id"}]
        get_table_mock.return_value = table_stub(columns, [])
        get_partitions_mock.return_value = []
        resp = generate_athena_queries(
            {
                "DataMapperId": "A",
                "QueryExecutor": "athena",
                "Columns": [col["Name"] for col in columns],
                "Format": "parquet",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test_db",
                    "Table": "test_table",
                },
            },
            [{"MatchId": "123", "DataMappers": ["B"], "DeletionQueueItemId": "id1234"}],
            "job_1234567890",
        )
        assert resp == []
        assert not put_object_mock.put_object.called

    @patch("backend.lambdas.tasks.generate_queries.s3.Bucket")
    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_removes_queries_with_no_applicable_matches_for_partitioned_data(
        self, get_partitions_mock, get_table_mock, bucket_mock
    ):
        put_object_mock = MagicMock()
        bucket_mock.return_value = put_object_mock
        columns = [{"Name": "customer_id"}]
        partition_keys = ["product_category"]
        partitions = [["Books"], ["Beauty"]]
        get_table_mock.return_value = table_stub(columns, partition_keys)
        get_partitions_mock.return_value = [
            partition_stub(p, columns) for p in partitions
        ]
        resp = generate_athena_queries(
            {
                "DataMapperId": "A",
                "QueryExecutor": "athena",
                "Columns": [col["Name"] for col in columns],
                "Format": "parquet",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test_db",
                    "Table": "test_table",
                },
            },
            [{"MatchId": "123", "DataMappers": ["C"], "DeletionQueueItemId": "id1234"}],
            "job_1234567890",
        )
        assert resp == []
        assert not put_object_mock.put_object.called

    @patch("backend.lambdas.tasks.generate_queries.glue_client")
    def test_it_returns_table(self, client):
        client.get_table.return_value = {"Table": {"Name": "test"}}
        result = get_table("test_db", "test_table")
        assert {"Name": "test"} == result
        client.get_table.assert_called_with(DatabaseName="test_db", Name="test_table")

    @patch("backend.lambdas.tasks.generate_queries.paginate")
    def test_it_returns_all_partitions(self, paginate):
        paginate.return_value = iter(["blah"])
        result = list(get_partitions("test_db", "test_table"))
        assert ["blah"] == result
        paginate.assert_called_with(
            mock.ANY,
            mock.ANY,
            ["Partitions"],
            **{"DatabaseName": "test_db", "TableName": "test_table"}
        )

    def test_it_converts_supported_types(self):
        for scenario in [
            {"value": "m", "type": "char", "expected": "m"},
            {"value": "mystr", "type": "string", "expected": "mystr"},
            {"value": "mystr", "type": "varchar", "expected": "mystr"},
            {"value": "2", "type": "bigint", "expected": 2},
            {"value": "2", "type": "int", "expected": 2},
            {"value": "2", "type": "smallint", "expected": 2},
            {"value": "2", "type": "tinyint", "expected": 2},
            {"value": "2.23", "type": "double", "expected": 2.23},
            {"value": "2.23", "type": "float", "expected": 2.23},
        ]:
            res = cast_to_type(
                scenario["value"],
                "test_col",
                {
                    "StorageDescriptor": {
                        "Columns": [{"Name": "test_col", "Type": scenario["type"]}]
                    }
                },
            )

            assert res == scenario["expected"]

    def test_it_converts_supported_types_when_nested_in_struct(self):
        column_type = "struct<type:int,x:map<string,struct<a:int>>,info:struct<user_id:int,name:string>>"
        table = {
            "StorageDescriptor": {"Columns": [{"Name": "user", "Type": column_type}]}
        }
        for scenario in [
            {"value": "john_doe", "id": "user.info.name", "expected": "john_doe"},
            {"value": "1234567890", "id": "user.info.user_id", "expected": 1234567890},
            {"value": "1", "id": "user.type", "expected": 1},
        ]:
            res = cast_to_type(scenario["value"], scenario["id"], table)
            assert res == scenario["expected"]

    def test_it_throws_for_unknown_col(self):
        with pytest.raises(ValueError):
            cast_to_type(
                "mystr",
                "doesnt_exist",
                {
                    "StorageDescriptor": {
                        "Columns": [{"Name": "test_col", "Type": "string"}]
                    }
                },
            )

    def test_it_throws_for_unsupported_complex_nested_types(self):
        for scenario in [
            "array<x:int>",
            "array<struct<x:int>>",
            "struct<a:array<struct<a:int,x:int>>>",
            "array<struct<a:int,b:struct<x:int>>>",
            "struct<a:map<string,struct<x:int>>>",
            "map<string,struct<x:int>>",
        ]:
            with pytest.raises(ValueError):
                cast_to_type(
                    123,
                    "user.x",
                    {
                        "StorageDescriptor": {
                            "Columns": [{"Name": "user", "Type": scenario}]
                        }
                    },
                )

    def test_it_throws_for_unsupported_col_types(self):
        with pytest.raises(ValueError) as e:
            cast_to_type(
                "2.56",
                "test_col",
                {
                    "StorageDescriptor": {
                        "Columns": [{"Name": "test_col", "Type": "foo"}]
                    }
                },
            )
        assert (
            e.value.args[0]
            == "Column test_col is not a supported column type for querying"
        )

    def test_it_throws_for_unconvertable_matches(self):
        with pytest.raises(ValueError):
            cast_to_type(
                "mystr",
                "test_col",
                {
                    "StorageDescriptor": {
                        "Columns": [{"Name": "test_col", "Type": "int"}]
                    }
                },
            )

    def test_it_throws_for_invalid_schema_for_inner_children(self):
        with pytest.raises(ValueError) as e:
            get_inner_children("struct<name:string", "struct<", ">")
        assert e.value.args[0] == "Column schema is not valid"

    def test_it_throws_for_invalid_schema_for_nested_children(self):
        with pytest.raises(ValueError) as e:
            get_nested_children(
                "struct<name:string,age:int,s:struct<n:int>,b:string", "struct"
            )
        assert e.value.args[0] == "Column schema is not valid"

    @patch("backend.lambdas.tasks.generate_queries.s3.Bucket")
    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_handles_partition_filtering(
        self, get_partitions_mock, get_table_mock, bucket_mock
    ):
        put_object_mock = MagicMock()
        bucket_mock.return_value = put_object_mock
        columns = [{"Name": "customer_id"}]
        partition_keys = ["year", "month"]
        partitions = [["2018", "12"], ["2019", "01"], ["2019", "02"]]
        get_table_mock.return_value = table_stub(columns, partition_keys)
        get_partitions_mock.return_value = [
            partition_stub(p, columns) for p in partitions
        ]

        resp = generate_athena_queries(
            {
                "DataMapperId": "a",
                "QueryExecutor": "athena",
                "Columns": [col["Name"] for col in columns],
                "Format": "parquet",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test_db",
                    "Table": "test_table",
                    "PartitionKeys": ["year"],
                },
            },
            [
                {
                    "MatchId": "hi",
                    "CreatedAt": 1614698440,
                    "DeletionQueueItemId": "id1234",
                }
            ],
            "job_1234567890",
        )

        assert lists_equal_ignoring_order(
            resp,
            [
                {
                    "DataMapperId": "a",
                    "QueryExecutor": "athena",
                    "Format": "parquet",
                    "Database": "test_db",
                    "Table": "test_table",
                    "Columns": [{"Column": "customer_id", "Type": "Simple"}],
                    "PartitionKeys": [{"Key": "year", "Value": "2018"}],
                    "DeleteOldVersions": True,
                    "IgnoreObjectNotFoundExceptions": False,
                    "Manifest": "s3://S3F2-manifests-bucket/manifests/job_1234567890/a/manifest.json",
                },
                {
                    "DataMapperId": "a",
                    "QueryExecutor": "athena",
                    "Format": "parquet",
                    "Database": "test_db",
                    "Table": "test_table",
                    "Columns": [{"Column": "customer_id", "Type": "Simple"}],
                    "PartitionKeys": [{"Key": "year", "Value": "2019"}],
                    "DeleteOldVersions": True,
                    "IgnoreObjectNotFoundExceptions": False,
                    "Manifest": "s3://S3F2-manifests-bucket/manifests/job_1234567890/a/manifest.json",
                },
            ],
        )
        put_object_mock.put_object.assert_called_with(
            Key="manifests/job_1234567890/a/manifest.json",
            Body=(
                json.dumps(
                    {
                        "Columns": ["customer_id"],
                        "MatchId": ["hi"],
                        "DeletionQueueItemId": "id1234",
                        "CreatedAt": 1614698440,
                        "QueryableColumns": "customer_id",
                        "QueryableMatchId": "hi",
                    }
                )
                + "\n"
            ),
        )


@patch("backend.lambdas.tasks.generate_queries.deserialize_item")
@patch("backend.lambdas.tasks.generate_queries.paginate")
def test_it_fetches_deletion_queue_from_ddb(paginate_mock, deserialize_mock):
    item = {"DeletionQueueItems": [{"DataMappers": [], "MatchId": "123"}]}
    deserialize_mock.return_value = item
    paginate_mock.return_value = iter([item])

    resp = get_deletion_queue()
    assert list(resp) == [item]


@patch("backend.lambdas.tasks.generate_queries.deserialize_item")
@patch("backend.lambdas.tasks.generate_queries.paginate")
def test_it_fetches_deserialized_data_mappers(paginate_mock, deserialize_mock):
    dm = {
        "DataMapperId": "a",
        "QueryExecutor": "athena",
        "Columns": ["customer_id"],
        "Format": "parquet",
        "QueryExecutorParameters": {
            "DataCatalogProvider": "glue",
            "Database": "test_db",
            "Table": "test_table",
        },
    }
    deserialize_mock.return_value = dm
    paginate_mock.return_value = iter([dm])

    resp = get_data_mappers()
    assert list(resp) == [dm]


@patch("backend.lambdas.tasks.generate_queries.glue_client")
def test_it_writes_glue_partitions(glue_client):
    write_partitions([["job_1234", "dm_0001"], ["job_1234", "dm_0003"]])
    glue_client.batch_create_partition.assert_called_with(
        DatabaseName="s3f2_manifests_database",
        TableName="s3f2_manifests_table",
        PartitionInputList=[
            {
                "Values": ["job_1234", "dm_0001"],
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "columns", "Type": "array<string>"},
                        {"Name": "matchid", "Type": "array<string>"},
                        {"Name": "deletionqueueitemid", "Type": "string"},
                        {"Name": "createdat", "Type": "int"},
                        {"Name": "queryablecolumns", "Type": "string"},
                        {"Name": "queryablematchid", "Type": "string"},
                    ],
                    "Location": "s3://S3F2-manifests-bucket/manifests/job_1234/dm_0001/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "Compressed": False,
                    "SerdeInfo": {
                        "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                    },
                    "StoredAsSubDirectories": False,
                },
            },
            {
                "Values": ["job_1234", "dm_0003"],
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "columns", "Type": "array<string>"},
                        {"Name": "matchid", "Type": "array<string>"},
                        {"Name": "deletionqueueitemid", "Type": "string"},
                        {"Name": "createdat", "Type": "int"},
                        {"Name": "queryablecolumns", "Type": "string"},
                        {"Name": "queryablematchid", "Type": "string"},
                    ],
                    "Location": "s3://S3F2-manifests-bucket/manifests/job_1234/dm_0003/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "Compressed": False,
                    "SerdeInfo": {
                        "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                    },
                    "StoredAsSubDirectories": False,
                },
            },
        ],
    )


def partition_stub(values, columns, table_name="test_table"):
    return {
        "Values": values,
        "DatabaseName": "test",
        "TableName": table_name,
        "CreationTime": 1572440736.0,
        "LastAccessTime": 0.0,
        "StorageDescriptor": {
            "Columns": [
                {"Name": col["Name"], "Type": col.get("Type", "string")}
                for col in columns
            ],
            "Location": "s3://bucket/location",
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "Compressed": False,
            "NumberOfBuckets": -1,
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
            "BucketColumns": [],
            "SortColumns": [],
            "Parameters": {},
            "SkewedInfo": {
                "SkewedColumnNames": [],
                "SkewedColumnValues": [],
                "SkewedColumnValueLocationMaps": {},
            },
            "StoredAsSubDirectories": False,
        },
    }


def table_stub(
    columns, partition_keys, table_name="test_table", partition_keys_type="string"
):
    return {
        "Name": table_name,
        "DatabaseName": "test",
        "Owner": "test",
        "CreateTime": 1572438253.0,
        "UpdateTime": 1572438253.0,
        "LastAccessTime": 0.0,
        "Retention": 0,
        "StorageDescriptor": {
            "Columns": [
                {"Name": col["Name"], "Type": col.get("Type", "string")}
                for col in columns
            ],
            "Location": "s3://bucket/location",
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "Compressed": False,
            "NumberOfBuckets": -1,
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
            "BucketColumns": [],
            "SortColumns": [],
            "Parameters": {},
            "SkewedInfo": {
                "SkewedColumnNames": [],
                "SkewedColumnValues": [],
                "SkewedColumnValueLocationMaps": {},
            },
            "StoredAsSubDirectories": False,
        },
        "PartitionKeys": [
            {"Name": partition_key, "Type": partition_keys_type}
            for partition_key in partition_keys
        ],
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"EXTERNAL": "TRUE",},
    }

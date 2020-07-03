import os
from types import SimpleNamespace

import mock
import pytest
from mock import patch

with patch.dict(os.environ, {"QueryQueue": "test"}):
    from backend.lambdas.tasks.generate_queries import handler, get_table, get_partitions, convert_to_col_type, \
        get_deletion_queue, generate_athena_queries, get_data_mappers

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.tasks.generate_queries.batch_sqs_msgs")
@patch("backend.lambdas.tasks.generate_queries.get_deletion_queue")
@patch("backend.lambdas.tasks.generate_queries.get_data_mappers")
@patch("backend.lambdas.tasks.generate_queries.generate_athena_queries")
def test_it_invokes_athena_query_generator(gen_athena_queries, get_data_mappers, get_del_q, batch_sqs_msgs_mock):
    get_del_q.return_value = [{"MatchId": "hi"}]
    queries = [{
        "DataMapperId": "a",
        "QueryExecutor": "athena",
        "Format": "parquet",
        "Database": "test_db",
        "Table": "test_table",
        "Columns": [{"Column": "customer_id", "MatchIds": ["hi"]}],
        "PartitionKeys": [{"Key": "product_category", "Value": "Books"}],
        "DeleteOldVersions": True
    }]
    gen_athena_queries.return_value = queries
    get_data_mappers.return_value = iter([{
        "DataMapperId": "a",
        "QueryExecutor": "athena",
        "Columns": ["customer_id"],
        "Format": "parquet",
        "QueryExecutorParameters": {
            "DataCatalogProvider": "glue",
            "Database": "test_db",
            "Table": "test_table"
        },
    }])
    handler({"ExecutionName": "test"}, SimpleNamespace())
    batch_sqs_msgs_mock.assert_called_with(mock.ANY, queries)


@patch("backend.lambdas.tasks.generate_queries.batch_sqs_msgs")
@patch("backend.lambdas.tasks.generate_queries.get_deletion_queue")
@patch("backend.lambdas.tasks.generate_queries.get_data_mappers")
def test_it_raises_for_unknown_query_executor(get_data_mappers, get_del_q, batch_sqs_msgs_mock):
    get_del_q.return_value = [{"MatchId": "hi"}]
    get_data_mappers.return_value = iter([{
        "DataMapperId": "a",
        "QueryExecutor": "invalid",
        "Columns": ["customer_id"],
        "Format": "parquet",
        "QueryExecutorParameters": {
            "DataCatalogProvider": "glue",
            "Database": "test_db",
            "Table": "test_table"
        },
    }])
    with pytest.raises(NotImplementedError):
        handler({"ExecutionName": "test"}, SimpleNamespace())
        batch_sqs_msgs_mock.assert_not_called()


class TestAthenaQueries:
    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_handles_single_columns(self, get_partitions_mock, get_table_mock):
        columns = ["customer_id"]
        partition_keys = ["product_category"]
        partitions = [["Books"]]
        get_table_mock.return_value = table_stub(columns, partition_keys)
        get_partitions_mock.return_value = [
            partition_stub(p, columns) for p in partitions
        ]
        resp = generate_athena_queries({
            "DataMapperId": "a",
            "QueryExecutor": "athena",
            "Columns": columns,
            "Format": "parquet",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test_db",
                "Table": "test_table"
            },
        }, [{"MatchId": "hi"}])
        assert resp == [{
            "DataMapperId": "a",
            "QueryExecutor": "athena",
            "Format": "parquet",
            "Database": "test_db",
            "Table": "test_table",
            "Columns": [{"Column": "customer_id", "MatchIds": ["hi"]}],
            "PartitionKeys": [{"Key": "product_category", "Value": "Books"}],
            "DeleteOldVersions": True
        }]

    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_handles_multiple_columns(self, get_partitions_mock, get_table_mock):
        columns = ["customer_id", "alt_customer_id"]
        partition_keys = ["product_category"]
        partitions = [["Books"]]
        get_table_mock.return_value = table_stub(columns, partition_keys)
        get_partitions_mock.return_value = [
            partition_stub(p, columns) for p in partitions
        ]
        resp = generate_athena_queries({
            "DataMapperId": "a",
            "QueryExecutor": "athena",
            "Columns": columns,
            "Format": "parquet",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test_db",
                "Table": "test_table"
            }
        }, [{"MatchId": "hi"}])

        assert resp == [{
            "DataMapperId": "a",
            "QueryExecutor": "athena",
            "Format": "parquet",
            "Database": "test_db",
            "Table": "test_table",
            "Columns": [
                {"Column": "customer_id", "MatchIds": ["hi"]},
                {"Column": "alt_customer_id", "MatchIds": ["hi"]}
            ],
            "PartitionKeys": [{"Key": "product_category", "Value": "Books"}],
            "DeleteOldVersions": True
        }]

    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_handles_multiple_partition_keys(self, get_partitions_mock, get_table_mock):
        columns = ["customer_id"]
        partition_keys = ["year", "month"]
        partitions = [["2019", "01"]]
        get_table_mock.return_value = table_stub(columns, partition_keys)
        get_partitions_mock.return_value = [
            partition_stub(p, columns) for p in partitions
        ]

        resp = generate_athena_queries({
            "DataMapperId": "a",
            "QueryExecutor": "athena",
            "Columns": columns,
            "Format": "parquet",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test_db",
                "Table": "test_table"
            }
        }, [{"MatchId": "hi"}])

        assert resp == [{
            "DataMapperId": "a",
            "QueryExecutor": "athena",
            "Format": "parquet",
            "Database": "test_db",
            "Table": "test_table",
            "Columns": [{"Column": "customer_id", "MatchIds": ["hi"]}],
            "PartitionKeys": [
                {"Key": "year", "Value": "2019"},
                {"Key": "month", "Value": "01"}
            ],
            "DeleteOldVersions": True
        }]

    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_handles_multiple_partition_values(self, get_partitions_mock, get_table_mock):
        columns = ["customer_id"]
        partition_keys = ["year", "month"]
        partitions = [["2018", "12"], ["2019", "01"], ["2019", "02"]]
        get_table_mock.return_value = table_stub(columns, partition_keys)
        get_partitions_mock.return_value = [
            partition_stub(p, columns) for p in partitions
        ]

        resp = generate_athena_queries({
            "DataMapperId": "a",
            "Columns": columns,
            "Format": "parquet",
            "QueryExecutor": "athena",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test_db",
                "Table": "test_table"
            }
        }, [{"MatchId": "hi"}])

        assert resp == [{
            "DataMapperId": "a",
            "Database": "test_db",
            "Table": "test_table",
            "QueryExecutor": "athena",
            "Format": "parquet",
            "Columns": [{"Column": "customer_id", "MatchIds": ["hi"]}],
            "PartitionKeys": [
                {"Key": "year", "Value": "2018"},
                {"Key": "month", "Value": "12"}
            ],
            "DeleteOldVersions": True
        },
        {
            "DataMapperId": "a",
            "Database": "test_db",
            "Table": "test_table",
            "QueryExecutor": "athena",
            "Format": "parquet",
            "Columns": [{"Column": "customer_id", "MatchIds": ["hi"]}],
            "PartitionKeys": [
                {"Key": "year", "Value": "2019"},
                {"Key": "month", "Value": "01"}
            ],
            "DeleteOldVersions": True
        },
        {
            "DataMapperId": "a",
            "Database": "test_db",
            "Table": "test_table",
            "QueryExecutor": "athena",
            "Format": "parquet",
            "Columns": [{"Column": "customer_id", "MatchIds": ["hi"]}],
            "PartitionKeys": [
                {"Key": "year", "Value": "2019"},
                {"Key": "month", "Value": "02"}
            ],
            "DeleteOldVersions": True
        }]

    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_propagates_optional_properties(self, get_partitions_mock, get_table_mock):
        columns = ["customer_id"]
        partition_keys = ["year", "month"]
        partitions = [["2018", "12"], ["2019", "01"]]
        get_table_mock.return_value = table_stub(columns, partition_keys)
        get_partitions_mock.return_value = [
            partition_stub(p, columns) for p in partitions
        ]

        resp = generate_athena_queries({
            "DataMapperId": "a",
            "QueryExecutor": "athena",
            "Columns": columns,
            "Format": "parquet",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test_db",
                "Table": "test_table"
            },
            "RoleArn": "arn:aws:iam::accountid:role/rolename",
            "DeleteOldVersions": True
        }, [{"MatchId": "hi"}])

        assert resp == [{
            "DataMapperId": "a",
            "Database": "test_db",
            "Table": "test_table",
            "QueryExecutor": "athena",
            "Format": "parquet",
            "Columns": [{"Column": "customer_id", "MatchIds": ["hi"]}],
            "PartitionKeys": [
                {"Key": "year", "Value": "2018"},
                {"Key": "month", "Value": "12"}
            ],
            "RoleArn": "arn:aws:iam::accountid:role/rolename",
            "DeleteOldVersions": True
        },
        {
            "DataMapperId": "a",
            "Database": "test_db",
            "Table": "test_table",
            "QueryExecutor": "athena",
            "Format": "parquet",
            "Columns": [{"Column": "customer_id", "MatchIds": ["hi"]}],
            "PartitionKeys": [
                {"Key": "year", "Value": "2019"},
                {"Key": "month", "Value": "01"}
            ],
            "RoleArn": "arn:aws:iam::accountid:role/rolename",
            "DeleteOldVersions": True
        }]

    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_filters_users_from_non_applicable_tables(self, get_partitions_mock, get_table_mock):
        columns = ["customer_id"]
        partition_keys = ["product_category"]
        partitions = [["Books"]]
        get_table_mock.return_value = table_stub(columns, partition_keys)
        get_partitions_mock.return_value = [
            partition_stub(p, columns) for p in partitions
        ]
        resp = generate_athena_queries({
            "DataMapperId": "B",
            "QueryExecutor": "athena",
            "Columns": columns,
            "Format": "parquet",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test_db",
                "Table": "B"
            }
        }, [
            {"MatchId": "123", "DataMappers": ["A"]},
            {"MatchId": "456", "DataMappers": []}
        ])

        assert resp == [{
            "DataMapperId": "B",
            "Database": "test_db",
            "Table": "B",
            "QueryExecutor": "athena",
            "Format": "parquet",
            "Columns": [{"Column": "customer_id", "MatchIds": ["456"]}],
            "PartitionKeys": [{"Key": "product_category", "Value": "Books"}],
            "DeleteOldVersions": True
        }]

    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_handles_unpartitioned_data(self, get_partitions_mock, get_table_mock):
        columns = ["customer_id"]
        get_table_mock.return_value = table_stub(columns, [])
        get_partitions_mock.return_value = []
        resp = generate_athena_queries({
            "DataMapperId": "a",
            "QueryExecutor": "athena",
            "Columns": columns,
            "Format": "parquet",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test_db",
                "Table": "test_table"
            }
        }, [{"MatchId": "hi"}])
        assert resp == [{
            "DataMapperId": "a",
            "Database": "test_db",
            "Table": "test_table",
            "QueryExecutor": "athena",
            "Format": "parquet",
            "Columns": [{"Column": "customer_id", "MatchIds": ["hi"]}],
            "PartitionKeys": [],
            "DeleteOldVersions": True,
        }]

    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_propagates_role_arn_for_unpartitioned_data(self, get_partitions_mock, get_table_mock):
        columns = ["customer_id"]
        get_table_mock.return_value = table_stub(columns, [])
        get_partitions_mock.return_value = []
        resp = generate_athena_queries({
            "DataMapperId": "a",
            "QueryExecutor": "athena",
            "Columns": columns,
            "Format": "parquet",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test_db",
                "Table": "test_table"
            },
            "RoleArn": "arn:aws:iam::accountid:role/rolename",
        }, [{"MatchId": "hi"}])
        assert resp == [{
            "DataMapperId": "a",
            "Database": "test_db",
            "Table": "test_table",
            "QueryExecutor": "athena",
            "Format": "parquet",
            "Columns": [{"Column": "customer_id", "MatchIds": ["hi"]}],
            "PartitionKeys": [],
            "RoleArn": "arn:aws:iam::accountid:role/rolename",
            "DeleteOldVersions": True,
        }]

    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_removes_queries_with_no_applicable_matches(self, get_partitions_mock, get_table_mock):
        columns = ["customer_id"]
        get_table_mock.return_value = table_stub(columns, [])
        get_partitions_mock.return_value = []
        resp = generate_athena_queries({
            "DataMapperId": "A",
            "QueryExecutor": "athena",
            "Columns": columns,
            "Format": "parquet",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test_db",
                "Table": "test_table"
            }
        }, [{"MatchId": "123", "DataMappers": ["B"]}])
        assert resp == []

    @patch("backend.lambdas.tasks.generate_queries.get_table")
    @patch("backend.lambdas.tasks.generate_queries.get_partitions")
    def test_it_removes_queries_with_no_applicable_matches_for_partitioned_data(
            self, get_partitions_mock, get_table_mock):
        columns = ["customer_id"]
        partition_keys = ["product_category"]
        partitions = [["Books"], ["Beauty"]]
        get_table_mock.return_value = table_stub(columns, partition_keys)
        get_partitions_mock.return_value = [
            partition_stub(p, columns) for p in partitions
        ]
        resp = generate_athena_queries({
            "DataMapperId": "A",
            "QueryExecutor": "athena",
            "Columns": columns,
            "Format": "parquet",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test_db",
                "Table": "test_table"
            }
        }, [{"MatchId": "123", "DataMappers": ["C"]}])
        assert resp == []

    @patch("backend.lambdas.tasks.generate_queries.glue_client")
    def test_it_returns_table(self, client):
        client.get_table.return_value = {"Table": {"Name": "test"}}
        result = get_table("test_db", "test_table")
        assert {"Name": "test"} == result
        client.get_table.assert_called_with(
            DatabaseName="test_db",
            Name="test_table"
        )

    @patch("backend.lambdas.tasks.generate_queries.paginate")
    def test_it_returns_all_partitions(self, paginate):
        paginate.return_value = iter(["blah"])
        result = list(get_partitions("test_db", "test_table"))
        assert ["blah"] == result
        paginate.assert_called_with(
            mock.ANY, mock.ANY, ["Partitions"], **{
                "DatabaseName": "test_db",
                "TableName": "test_table"
            }
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
            {"value": "2.23", "type": "float", "expected": 2.23}]:
            res = convert_to_col_type(scenario["value"], "test_col", {
                "StorageDescriptor": {
                    "Columns": [{
                        "Name": "test_col",
                        "Type": scenario["type"]
                    }]
                }
            })

            assert res == scenario["expected"]


    def test_it_throws_for_unknown_col(self):
        with pytest.raises(ValueError):
            convert_to_col_type("mystr", "doesnt_exist", {"StorageDescriptor": {"Columns": [{
                "Name": "test_col",
                "Type": "string"
            }]}})

    def test_it_throws_for_unsupported_col_types(self):
        with pytest.raises(ValueError):
            convert_to_col_type("2.56", "test_col", {"StorageDescriptor": {"Columns": [{
                "Name": "test_col",
                "Type": "decimal"
            }]}})

    def test_it_throws_for_unconvertable_matches(self):
        with pytest.raises(ValueError):
            convert_to_col_type("mystr", "test_col", {"StorageDescriptor": {"Columns": [{
                "Name": "test_col",
                "Type": "int"
            }]}})


@patch("backend.lambdas.tasks.generate_queries.jobs_table")
def test_it_fetches_deletion_queue_from_ddb(table_mock):
    table_mock.get_item.return_value = {
        "Item": {
            "DeletionQueueItems": [{
                "DataMappers": [],
                "MatchId": "123"
            }]
        }
    }

    resp = get_deletion_queue("job123")
    assert resp == [{
        'DataMappers': [],
        'MatchId': '123'
    }]
    table_mock.get_item.assert_called_with(Key={'Id': 'job123', 'Sk': 'job123'})


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
            "Table": "test_table"
        },
    }
    deserialize_mock.return_value = dm
    paginate_mock.return_value = iter([dm])

    resp = get_data_mappers()
    assert list(resp) == [dm]


def partition_stub(values, columns, table_name="test_table"):
    return {
        "Values": values,
        "DatabaseName": "test",
        "TableName": table_name,
        "CreationTime": 1572440736.0,
        "LastAccessTime": 0.0,
        "StorageDescriptor": {
            "Columns": [
                {
                    "Name": col,
                    "Type": "string"
                } for col in columns
            ],
            "Location": "s3://bucket/location",
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "Compressed": False,
            "NumberOfBuckets": -1,
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {
                    "serialization.format": "1"
                }
            },
            "BucketColumns": [],
            "SortColumns": [],
            "Parameters": {},
            "SkewedInfo": {
                "SkewedColumnNames": [],
                "SkewedColumnValues": [],
                "SkewedColumnValueLocationMaps": {}
            },
            "StoredAsSubDirectories": False
        }
    }


def table_stub(columns, partition_keys, table_name="test_table"):
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
                {
                    "Name": col,
                    "Type": "string"
                } for col in columns
            ],
            "Location": "s3://bucket/location",
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "Compressed": False,
            "NumberOfBuckets": -1,
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {
                    "serialization.format": "1"
                }
            },
            "BucketColumns": [],
            "SortColumns": [],
            "Parameters": {},
            "SkewedInfo": {
                "SkewedColumnNames": [],
                "SkewedColumnValues": [],
                "SkewedColumnValueLocationMaps": {}
            },
            "StoredAsSubDirectories": False
        },
        "PartitionKeys": [
            {
                "Name": partition_key,
                "Type": "string"
            } for partition_key in partition_keys
        ],
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "EXTERNAL": "TRUE",
        },
    }

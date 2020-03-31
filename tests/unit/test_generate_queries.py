import os
from types import SimpleNamespace

import mock
import pytest
from mock import patch

with patch.dict(os.environ, {"QueryQueue": "test"}):
    from backend.lambdas.tasks.generate_queries import handler, get_table, get_partitions, convert_to_col_type

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.tasks.generate_queries.get_table")
@patch("backend.lambdas.tasks.generate_queries.get_partitions")
@patch("backend.lambdas.tasks.generate_queries.batch_sqs_msgs")
def test_it_handles_single_columns(batch_sqs_msgs_mock, get_partitions_mock, get_table_mock):
    columns = ["customer_id"]
    partition_keys = ["product_category"]
    partitions = [["Books"]]
    get_table_mock.return_value = table_stub(columns, partition_keys)
    get_partitions_mock.return_value = [
        partition_stub(p, columns) for p in partitions
    ]
    handler({
        "DataMappers": [{
            "DataMapperId": "a",
            "QueryExecutor": "athena",
            "Columns": columns,
            "Format": "parquet",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test_db",
                "Table": "test_table"
            }
        }],
        "DeletionQueue": [{
            "MatchId": "hi",
        }]
    }, SimpleNamespace())

    batch_sqs_msgs_mock.assert_called_with(mock.ANY, [{
        "DataMapperId": "a",
        'Database': 'test_db',
        'Table': 'test_table',
        'Columns': [{'Column': 'customer_id', 'MatchIds': ['hi']}],
        'PartitionKeys': [{'Key': 'product_category', 'Value': 'Books'}]
    }])


@patch("backend.lambdas.tasks.generate_queries.get_table")
@patch("backend.lambdas.tasks.generate_queries.get_partitions")
@patch("backend.lambdas.tasks.generate_queries.batch_sqs_msgs")
def test_it_handles_multiple_columns(batch_sqs_msgs_mock, get_partitions_mock, get_table_mock):
    columns = ["customer_id", "alt_customer_id"]
    partition_keys = ["product_category"]
    partitions = [["Books"]]
    get_table_mock.return_value = table_stub(columns, partition_keys)
    get_partitions_mock.return_value = [
        partition_stub(p, columns) for p in partitions
    ]
    handler({
        "DataMappers": [{
            "DataMapperId": "a",
            "QueryExecutor": "athena",
            "Columns": columns,
            "Format": "parquet",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test_db",
                "Table": "test_table"
            }
        }],
        "DeletionQueue": [{
            "MatchId": "hi",
        }]
    }, SimpleNamespace())

    batch_sqs_msgs_mock.assert_called_with(mock.ANY, [
        {
            "DataMapperId": "a",
            'Database': 'test_db',
            'Table': 'test_table',
            'Columns': [
                {'Column': 'customer_id', 'MatchIds': ['hi']},
                {'Column': 'alt_customer_id', 'MatchIds': ['hi']}],
            'PartitionKeys': [{'Key': 'product_category', 'Value': 'Books'}]
        }
    ])


@patch("backend.lambdas.tasks.generate_queries.get_table")
@patch("backend.lambdas.tasks.generate_queries.get_partitions")
@patch("backend.lambdas.tasks.generate_queries.batch_sqs_msgs")
def test_it_handles_multiple_partition_keys(batch_sqs_msgs_mock, get_partitions_mock, get_table_mock):
    columns = ["customer_id"]
    partition_keys = ["year", "month"]
    partitions = [["2019", "01"]]
    get_table_mock.return_value = table_stub(columns, partition_keys)
    get_partitions_mock.return_value = [
        partition_stub(p, columns) for p in partitions
    ]
    handler({
        "DataMappers": [{
            "DataMapperId": "a",
            "QueryExecutor": "athena",
            "Columns": columns,
            "Format": "parquet",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test_db",
                "Table": "test_table"
            }
        }],
        "DeletionQueue": [{
            "MatchId": "hi",
        }]
    }, SimpleNamespace())

    batch_sqs_msgs_mock.assert_called_with(mock.ANY, [
        {
            "DataMapperId": "a",
            'Database': 'test_db',
            'Table': 'test_table',
            'Columns': [{'Column': 'customer_id', 'MatchIds': ['hi']}],
            'PartitionKeys': [
                {'Key': 'year', 'Value': '2019'},
                {'Key': 'month', 'Value': '01'}
            ]
        }
    ])


@patch("backend.lambdas.tasks.generate_queries.get_table")
@patch("backend.lambdas.tasks.generate_queries.get_partitions")
@patch("backend.lambdas.tasks.generate_queries.batch_sqs_msgs")
def test_it_handles_multiple_partition_values(batch_sqs_msgs_mock, get_partitions_mock, get_table_mock):
    columns = ["customer_id"]
    partition_keys = ["year", "month"]
    partitions = [["2018", "12"], ["2019", "01"], ["2019", "02"]]
    get_table_mock.return_value = table_stub(columns, partition_keys)
    get_partitions_mock.return_value = [
        partition_stub(p, columns) for p in partitions
    ]
    handler({
        "DataMappers": [{
            "DataMapperId": "a",
            "QueryExecutor": "athena",
            "Columns": columns,
            "Format": "parquet",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test_db",
                "Table": "test_table"
            }
        }],
        "DeletionQueue": [{
            "MatchId": "hi",
        }]
    }, SimpleNamespace())

    batch_sqs_msgs_mock.assert_called_with(mock.ANY, [
        {
            "DataMapperId": "a",
            'Database': 'test_db',
            'Table': 'test_table',
            'Columns': [{'Column': 'customer_id', 'MatchIds': ['hi']}],
            'PartitionKeys': [
                {'Key': 'year', 'Value': '2018'},
                {'Key': 'month', 'Value': '12'}
            ]
        },
        {
            "DataMapperId": "a",
            'Database': 'test_db',
            'Table': 'test_table',
            'Columns': [{'Column': 'customer_id', 'MatchIds': ['hi']}],
            'PartitionKeys': [
                {'Key': 'year', 'Value': '2019'},
                {'Key': 'month', 'Value': '01'}
            ]
        },
        {
            "DataMapperId": "a",
            'Database': 'test_db',
            'Table': 'test_table',
            'Columns': [{'Column': 'customer_id', 'MatchIds': ['hi']}],
            'PartitionKeys': [
                {'Key': 'year', 'Value': '2019'},
                {'Key': 'month', 'Value': '02'}
            ]
        }
    ])


@patch("backend.lambdas.tasks.generate_queries.get_table")
@patch("backend.lambdas.tasks.generate_queries.get_partitions")
@patch("backend.lambdas.tasks.generate_queries.batch_sqs_msgs")
def test_it_propagates_role_for_partitioned_data(batch_sqs_msgs_mock, get_partitions_mock, get_table_mock):
    columns = ["customer_id"]
    partition_keys = ["year", "month"]
    partitions = [["2018", "12"], ["2019", "01"]]
    get_table_mock.return_value = table_stub(columns, partition_keys)
    get_partitions_mock.return_value = [
        partition_stub(p, columns) for p in partitions
    ]
    handler({
        "DataMappers": [{
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
        }],
        "DeletionQueue": [{
            "MatchId": "hi",
        }]
    }, SimpleNamespace())

    batch_sqs_msgs_mock.assert_called_with(mock.ANY, [
        {
            "DataMapperId": "a",
            'Database': 'test_db',
            'Table': 'test_table',
            'Columns': [{'Column': 'customer_id', 'MatchIds': ['hi']}],
            'PartitionKeys': [
                {'Key': 'year', 'Value': '2018'},
                {'Key': 'month', 'Value': '12'}
            ],
            "RoleArn": "arn:aws:iam::accountid:role/rolename",
        },
        {
            "DataMapperId": "a",
            'Database': 'test_db',
            'Table': 'test_table',
            'Columns': [{'Column': 'customer_id', 'MatchIds': ['hi']}],
            'PartitionKeys': [
                {'Key': 'year', 'Value': '2019'},
                {'Key': 'month', 'Value': '01'}
            ],
            "RoleArn": "arn:aws:iam::accountid:role/rolename",
        }
    ])


@patch("backend.lambdas.tasks.generate_queries.get_table")
@patch("backend.lambdas.tasks.generate_queries.get_partitions")
@patch("backend.lambdas.tasks.generate_queries.batch_sqs_msgs")
def test_it_filters_users_from_non_applicable_tables(batch_sqs_msgs_mock, get_partitions_mock, get_table_mock):
    columns = ["customer_id"]
    partition_keys = ["product_category"]
    partitions = [["Books"]]
    get_table_mock.return_value = table_stub(columns, partition_keys)
    get_partitions_mock.return_value = [
        partition_stub(p, columns) for p in partitions
    ]
    handler({
        "DataMappers": [{
            "DataMapperId": "A",
            "QueryExecutor": "athena",
            "Columns": columns,
            "Format": "parquet",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test_db",
                "Table": "A"
            }
        },
        {
            "DataMapperId": "B",
            "QueryExecutor": "athena",
            "Columns": columns,
            "Format": "parquet",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test_db",
                "Table": "B"
            }
        }],
        "DeletionQueue": [{
            "MatchId": "123",
            "DataMappers": [
                "A"
            ]
        }, {
            "MatchId": "456",
            "DataMappers": []
        }]
    }, SimpleNamespace())

    batch_sqs_msgs_mock.assert_any_call(mock.ANY, [
        {
            "DataMapperId": "A",
            'Database': 'test_db',
            'Table': 'A',
            'Columns': [{'Column': 'customer_id', 'MatchIds': ['123', '456']}],
            'PartitionKeys': [{'Key': 'product_category', 'Value': 'Books'}]
        }
    ]),
    batch_sqs_msgs_mock.assert_any_call(mock.ANY, [
        {
            "DataMapperId": "B",
            'Database': 'test_db',
            'Table': 'B',
            'Columns': [{'Column': 'customer_id', 'MatchIds': ['456']}],
            'PartitionKeys': [{'Key': 'product_category', 'Value': 'Books'}]
        }
    ])


@patch("backend.lambdas.tasks.generate_queries.get_table")
@patch("backend.lambdas.tasks.generate_queries.get_partitions")
@patch("backend.lambdas.tasks.generate_queries.batch_sqs_msgs")
def test_it_handles_unpartitioned_data(batch_sqs_msgs_mock, get_partitions_mock, get_table_mock):
    columns = ["customer_id"]
    get_table_mock.return_value = table_stub(columns, [])
    get_partitions_mock.return_value = []
    handler({
        "DataMappers": [{
            "DataMapperId": "a",
            "QueryExecutor": "athena",
            "Columns": columns,
            "Format": "parquet",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test_db",
                "Table": "test_table"
            }
        }],
        "DeletionQueue": [{
            "MatchId": "123",
        }]
    }, SimpleNamespace())

    batch_sqs_msgs_mock.assert_called_with(mock.ANY, [
        {
            "DataMapperId": "a",
            'Database': 'test_db',
            'Table': 'test_table',
            'Columns': [{'Column': 'customer_id', 'MatchIds': ['123']}],
            'PartitionKeys': [],
        },
    ])


@patch("backend.lambdas.tasks.generate_queries.get_table")
@patch("backend.lambdas.tasks.generate_queries.get_partitions")
@patch("backend.lambdas.tasks.generate_queries.batch_sqs_msgs")
def test_it_propagates_role_arn_for_unpartitioned_data(batch_sqs_msgs_mock, get_partitions_mock, get_table_mock):
    columns = ["customer_id"]
    get_table_mock.return_value = table_stub(columns, [])
    get_partitions_mock.return_value = []
    handler({
        "DataMappers": [{
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
        }],
        "DeletionQueue": [{
            "MatchId": "123",
        }]
    }, SimpleNamespace())

    batch_sqs_msgs_mock.assert_called_with(mock.ANY, [
        {
            "DataMapperId": "a",
            'Database': 'test_db',
            'Table': 'test_table',
            'Columns': [{'Column': 'customer_id', 'MatchIds': ['123']}],
            'PartitionKeys': [],
            "RoleArn": "arn:aws:iam::accountid:role/rolename",
        },
    ])


@patch("backend.lambdas.tasks.generate_queries.get_table")
@patch("backend.lambdas.tasks.generate_queries.get_partitions")
@patch("backend.lambdas.tasks.generate_queries.batch_sqs_msgs")
def test_it_removes_queries_with_no_applicable_matches(batch_sqs_msgs_mock, get_partitions_mock, get_table_mock):
    columns = ["customer_id"]
    get_table_mock.return_value = table_stub(columns, [])
    get_partitions_mock.return_value = []
    handler({
        "DataMappers": [{
            "DataMapperId": "A",
            "QueryExecutor": "athena",
            "Columns": columns,
            "Format": "parquet",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test_db",
                "Table": "test_table"
            }
        }],
        "DeletionQueue": [{
            "MatchId": "123",
            "DataMappers": ["B"]
        }]
    }, SimpleNamespace())

    batch_sqs_msgs_mock.assert_called_with(mock.ANY, [])


@patch("backend.lambdas.tasks.generate_queries.get_table")
@patch("backend.lambdas.tasks.generate_queries.get_partitions")
@patch("backend.lambdas.tasks.generate_queries.batch_sqs_msgs")
def test_it_removes_queries_with_no_applicable_matches_for_partitioned_data(
        batch_sqs_msgs_mock, get_partitions_mock, get_table_mock):
    columns = ["customer_id"]
    partition_keys = ["product_category"]
    partitions = [["Books"], ["Beauty"]]
    get_table_mock.return_value = table_stub(columns, partition_keys)
    get_partitions_mock.return_value = [
        partition_stub(p, columns) for p in partitions
    ]
    handler({
        "DataMappers": [{
            "DataMapperId": "A",
            "QueryExecutor": "athena",
            "Columns": columns,
            "Format": "parquet",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test_db",
                "Table": "test_table"
            }
        }],
        "DeletionQueue": [{
            "MatchId": "123",
            "DataMappers": ["C"]
        }]
    }, SimpleNamespace())

    batch_sqs_msgs_mock.assert_called_with(mock.ANY, [])


@patch("backend.lambdas.tasks.generate_queries.glue_client")
def test_it_returns_table(client):
    client.get_table.return_value = {"Table": {"Name": "test"}}
    result = get_table("test_db", "test_table")
    assert {"Name": "test"} == result
    client.get_table.assert_called_with(
        DatabaseName="test_db",
        Name="test_table"
    )


@patch("backend.lambdas.tasks.generate_queries.paginate")
def test_it_returns_all_partitions(paginate):
    paginate.return_value = iter(["blah"])
    result = get_partitions("test_db", "test_table")
    assert ["blah"] == result
    paginate.assert_called_with(
        mock.ANY, mock.ANY, ["Partitions"], **{
            "DatabaseName": "test_db",
            "TableName": "test_table"
        }
    )


def test_it_converts_strings():
    res = convert_to_col_type("mystr", "test_col", {"StorageDescriptor": {"Columns": [{
        "Name": "test_col",
        "Type": "string"
    }]}})
    assert "mystr" == res


def test_it_converts_varchar():
    res = convert_to_col_type("mystr", "test_col", {"StorageDescriptor": {"Columns": [{
        "Name": "test_col",
        "Type": "varchar"
    }]}})
    assert "mystr" == res


def test_it_converts_ints():
    res = convert_to_col_type("2", "test_col", {"StorageDescriptor": {"Columns": [{
        "Name": "test_col",
        "Type": "int"
    }]}})
    assert 2 == res


def test_it_converts_bigints():
    res = convert_to_col_type("1572438253", "test_col", {"StorageDescriptor": {"Columns": [{
        "Name": "test_col",
        "Type": "bigint"
    }]}})
    assert 1572438253 == res


def test_it_throws_for_unknown_col():
    with pytest.raises(ValueError):
        convert_to_col_type("mystr", "doesnt_exist", {"StorageDescriptor": {"Columns": [{
            "Name": "test_col",
            "Type": "string"
        }]}})


def test_it_throws_for_unsupported_col_types():
    with pytest.raises(ValueError):
        convert_to_col_type("mystr", "test_col", {"StorageDescriptor": {"Columns": [{
            "Name": "test_col",
            "Type": "map"
        }]}})


def test_it_throws_for_unconvertable_matches():
    with pytest.raises(ValueError):
        convert_to_col_type("mystr", "test_col", {"StorageDescriptor": {"Columns": [{
            "Name": "test_col",
            "Type": "int"
        }]}})


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

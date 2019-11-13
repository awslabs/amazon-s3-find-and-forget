from types import SimpleNamespace

import mock
import pytest
from mock import patch

from lambdas.src.tasks.generate_queries import handler, get_table, get_partitions

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("lambdas.src.tasks.generate_queries.get_table")
@patch("lambdas.src.tasks.generate_queries.get_partitions")
def test_it_handles_single_columns(get_partitions_mock, get_table_mock):
    columns = ["customer_id"]
    partition_keys = ["product_category"]
    partitions = [["Books"]]
    get_table_mock.return_value = table_stub(columns, partition_keys)
    get_partitions_mock.return_value = [
        partition_stub(p, columns) for p in partitions
    ]
    result = handler({
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

    assert [
        {
            "DataMapperId": "a",
            'Database': 'test_db',
            'Table': 'test_table',
            'Columns': [{'Column': 'customer_id', 'MatchIds': ['hi']}],
            'PartitionKeys': [{'Key': 'product_category', 'Value': 'Books'}]
        }
    ] == result


@patch("lambdas.src.tasks.generate_queries.get_table")
@patch("lambdas.src.tasks.generate_queries.get_partitions")
def test_it_handles_multiple_columns(get_partitions_mock, get_table_mock):
    columns = ["customer_id", "alt_customer_id"]
    partition_keys = ["product_category"]
    partitions = [["Books"]]
    get_table_mock.return_value = table_stub(columns, partition_keys)
    get_partitions_mock.return_value = [
        partition_stub(p, columns) for p in partitions
    ]
    result = handler({
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

    assert [
        {
            "DataMapperId": "a",
            'Database': 'test_db',
            'Table': 'test_table',
            'Columns': [
                {'Column': 'customer_id', 'MatchIds': ['hi']},
                {'Column': 'alt_customer_id', 'MatchIds': ['hi']}],
            'PartitionKeys': [{'Key': 'product_category', 'Value': 'Books'}]
        }
    ] == result


@patch("lambdas.src.tasks.generate_queries.get_table")
@patch("lambdas.src.tasks.generate_queries.get_partitions")
def test_it_handles_multiple_partition_keys(get_partitions_mock, get_table_mock):
    columns = ["customer_id"]
    partition_keys = ["year", "month"]
    partitions = [["2019", "01"]]
    get_table_mock.return_value = table_stub(columns, partition_keys)
    get_partitions_mock.return_value = [
        partition_stub(p, columns) for p in partitions
    ]
    result = handler({
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

    assert [
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
    ] == result


@patch("lambdas.src.tasks.generate_queries.get_table")
@patch("lambdas.src.tasks.generate_queries.get_partitions")
def test_it_handles_multiple_partition_values(get_partitions_mock, get_table_mock):
    columns = ["customer_id"]
    partition_keys = ["year", "month"]
    partitions = [["2018", "12"], ["2019", "01"], ["2019", "02"]]
    get_table_mock.return_value = table_stub(columns, partition_keys)
    get_partitions_mock.return_value = [
        partition_stub(p, columns) for p in partitions
    ]
    result = handler({
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

    assert [
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
    ] == result


@patch("lambdas.src.tasks.generate_queries.get_table")
@patch("lambdas.src.tasks.generate_queries.get_partitions")
def test_it_filters_users_from_non_applicable_tables(get_partitions_mock, get_table_mock):
    columns = ["customer_id"]
    partition_keys = ["product_category"]
    partitions = [["Books"]]
    get_table_mock.return_value = table_stub(columns, partition_keys)
    get_partitions_mock.return_value = [
        partition_stub(p, columns) for p in partitions
    ]
    result = handler({
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

    assert [
        {
            "DataMapperId": "A",
            'Database': 'test_db',
            'Table': 'A',
            'Columns': [{'Column': 'customer_id', 'MatchIds': ['123', '456']}],
            'PartitionKeys': [{'Key': 'product_category', 'Value': 'Books'}]
        },
        {
            "DataMapperId": "B",
            'Database': 'test_db',
            'Table': 'B',
            'Columns': [{'Column': 'customer_id', 'MatchIds': ['456']}],
            'PartitionKeys': [{'Key': 'product_category', 'Value': 'Books'}]
        }
    ] == result


@patch("lambdas.src.tasks.generate_queries.get_table")
@patch("lambdas.src.tasks.generate_queries.get_partitions")
def test_it_handles_unpartitioned_data(get_partitions_mock, get_table_mock):
    columns = ["customer_id"]
    get_table_mock.return_value = table_stub(columns, [])
    get_partitions_mock.return_value = []
    result = handler({
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

    assert [
        {
            "DataMapperId": "a",
            'Database': 'test_db',
            'Table': 'test_table',
            'Columns': [{'Column': 'customer_id', 'MatchIds': ['123']}],
        },
    ] == result


@patch("lambdas.src.tasks.generate_queries.get_table")
@patch("lambdas.src.tasks.generate_queries.get_partitions")
def test_it_removes_queries_with_no_applicable_matches(get_partitions_mock, get_table_mock):
    columns = ["customer_id"]
    get_table_mock.return_value = table_stub(columns, [])
    get_partitions_mock.return_value = []
    result = handler({
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

    assert [] == result


@patch("lambdas.src.tasks.generate_queries.client")
def test_it_returns_table(client):
    client.get_table.return_value = {"Table": {"Name": "test"}}
    result = get_table("test_db", "test_table")
    assert {"Name": "test"} == result
    client.get_table.assert_called_with(
        DatabaseName="test_db",
        Name="test_table"
    )


@patch("lambdas.src.tasks.generate_queries.paginate")
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

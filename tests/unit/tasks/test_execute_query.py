import os
import re
from types import SimpleNamespace

import pytest
from mock import patch

from backend.lambdas.tasks.execute_query import handler, make_query, escape_item

pytestmark = [pytest.mark.unit, pytest.mark.task]


def escape_resp(resp):
    """
    Escape the given string.

    Args:
        resp: (str): write your description
    """
    return re.sub("[\x00-\x20]+", " ", resp.strip())


@patch("backend.lambdas.tasks.execute_query.client")
@patch("backend.lambdas.tasks.execute_query.make_query")
def test_it_executes_queries(query_mock, client_mock):
    """
    Test for queries that have a mock.

    Args:
        query_mock: (todo): write your description
        client_mock: (todo): write your description
    """
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
    """
    Get custom custom custom permits for a mock.

    Args:
        query_mock: (str): write your description
        client_mock: (todo): write your description
    """
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
    """
    Test if the query : param partition :

    Args:
    """
    resp = make_query(
        {
            "Database": "amazonreviews",
            "Table": "amazon_reviews_parquet",
            "Columns": [{"Column": "customer_id", "MatchIds": ["123456", "456789"]}],
            "PartitionKeys": [{"Key": "product_category", "Value": "Books"}],
        }
    )

    assert (
        escape_resp(resp) == 'SELECT DISTINCT "$path" '
        'FROM "amazonreviews"."amazon_reviews_parquet" '
        "WHERE (\"customer_id\" in ('123456', '456789')) "
        "AND \"product_category\" = 'Books'"
    )


def test_it_generates_query_with_partition_and_int_column():
    """
    : param int partition_it_query_column_int_int - query_part_column_column_column_query_column - test

    Args:
    """
    resp = make_query(
        {
            "Database": "amazonreviews",
            "Table": "amazon_reviews_parquet",
            "Columns": [{"Column": "customer_id", "MatchIds": [123456, 456789]}],
            "PartitionKeys": [{"Key": "product_category", "Value": "Books"}],
        }
    )

    assert (
        escape_resp(resp) == 'SELECT DISTINCT "$path" '
        'FROM "amazonreviews"."amazon_reviews_parquet" '
        'WHERE ("customer_id" in (123456, 456789)) '
        "AND \"product_category\" = 'Books'"
    )


def test_it_generates_query_with_int_partition():
    """
    Test if the query :

    Args:
    """
    resp = make_query(
        {
            "Database": "amazonreviews",
            "Table": "amazon_reviews_parquet",
            "Columns": [{"Column": "customer_id", "MatchIds": ["123456", "456789"]}],
            "PartitionKeys": [{"Key": "year", "Value": 2010}],
        }
    )

    assert (
        escape_resp(resp) == 'SELECT DISTINCT "$path" '
        'FROM "amazonreviews"."amazon_reviews_parquet" '
        "WHERE (\"customer_id\" in ('123456', '456789')) "
        'AND "year" = 2010'
    )


def test_it_generates_query_with_multiple_partitions():
    """
    Test if the query part_it_query.

    Args:
    """
    resp = make_query(
        {
            "Database": "amazonreviews",
            "Table": "amazon_reviews_parquet",
            "Columns": [{"Column": "customer_id", "MatchIds": ["123456", "456789"]}],
            "PartitionKeys": [
                {"Key": "product_category", "Value": "Books"},
                {"Key": "published", "Value": "2019"},
            ],
        }
    )

    assert (
        escape_resp(resp) == 'SELECT DISTINCT "$path" '
        'FROM "amazonreviews"."amazon_reviews_parquet" '
        "WHERE (\"customer_id\" in ('123456', '456789')) "
        "AND \"product_category\" = 'Books' "
        "AND \"published\" = '2019'"
    )


def test_it_generates_query_without_partition():
    """
    Test if the query_without query :

    Args:
    """
    resp = make_query(
        {
            "Database": "amazonreviews",
            "Table": "amazon_reviews_parquet",
            "Columns": [{"Column": "customer_id", "MatchIds": ["123456", "456789"]}],
        }
    )

    assert (
        escape_resp(resp) == 'SELECT DISTINCT "$path" '
        'FROM "amazonreviews"."amazon_reviews_parquet" '
        "WHERE (\"customer_id\" in ('123456', '456789'))"
    )


def test_it_generates_query_with_multiple_columns():
    """
    : param query_it_query_it_query

    Args:
    """
    resp = make_query(
        {
            "Database": "amazonreviews",
            "Table": "amazon_reviews_parquet",
            "Columns": [
                {"Column": "a", "MatchIds": ["a123456", "b123456"]},
                {"Column": "b", "MatchIds": ["a456789", "b456789"]},
            ],
        }
    )

    assert (
        escape_resp(resp) == 'SELECT DISTINCT "$path" '
        'FROM "amazonreviews"."amazon_reviews_parquet" '
        "WHERE (\"a\" in ('a123456', 'b123456') OR \"b\" in ('a456789', 'b456789'))"
    )


def test_it_generates_query_with_columns_of_complex_type():
    """
    : return a_query_it_query_query_query.

    Args:
    """
    resp = make_query(
        {
            "Database": "amazonreviews",
            "Table": "amazon_reviews_parquet",
            "Columns": [{"Column": "a.b.c", "MatchIds": ["a123456", "b123456"]}],
        }
    )

    assert (
        escape_resp(resp) == 'SELECT DISTINCT "$path" '
        'FROM "amazonreviews"."amazon_reviews_parquet" '
        'WHERE ("a"."b"."c" in (\'a123456\', \'b123456\'))'
    )


def test_it_escapes_strings():
    """
    Escape a string : param string : : return :

    Args:
    """
    assert "''' OR 1=1'" == escape_item("' OR 1=1")


def test_it_escapes_ints():
    """
    Eval : attr.

    Args:
    """
    assert 2 == escape_item(2)


def test_it_escapes_floats():
    """
    Test if any number of digits.

    Args:
    """
    assert float(2) == escape_item(float(2))


def test_it_escapes_none():
    """
    Evaluate a string.

    Args:
    """
    assert "NULL" == escape_item(None)


def test_it_raises_for_unsupported_type():
    """
    Determine whether the test types.

    Args:
    """
    with pytest.raises(ValueError):
        escape_item(["val"])

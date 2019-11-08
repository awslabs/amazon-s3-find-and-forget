import re
from types import SimpleNamespace

import pytest
from mock import patch

from lambdas.src.tasks.execute_query import handler, make_query, escape_item

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("lambdas.src.tasks.execute_query.client")
@patch("lambdas.src.tasks.execute_query.make_query")
def test_it_executes_queries(query_mock, client_mock):
    client_mock.start_query_execution.return_value = {
        "QueryExecutionId": "123"
    }
    query_mock.return_value = "test"

    resp = handler({"QueryData": {}, "Bucket": "mybucket", "Prefix": "my_prefix"}, SimpleNamespace())
    assert "123" == resp
    client_mock.start_query_execution.assert_called_with(QueryString="test", ResultConfiguration={
        'OutputLocation': 's3://mybucket/my_prefix/'
    })


def test_it_generates_query_with_partition():
    resp = make_query({
        "Database": "amazonreviews",
        "Table": "amazon_reviews_parquet",
        "Columns": [{"Column": "customer_id", "MatchIds": ["123456", "456789"]}],
        "PartitionKeys": [{"Key": "product_category", "Value": "Books"}]
    })

    assert "SELECT \"$path\" " \
           "FROM \"amazonreviews\".\"amazon_reviews_parquet\" " \
           "WHERE (\"customer_id\" in ('123456', '456789')) " \
           "AND \"product_category\" = 'Books'" == re.sub("[\x00-\x20]+", " ", resp.strip())


def test_it_generates_query_with_multiple_partitions():
    resp = make_query({
        "Database": "amazonreviews",
        "Table": "amazon_reviews_parquet",
        "Columns": [{"Column": "customer_id", "MatchIds": ["123456", "456789"]}],
        "PartitionKeys": [{"Key": "product_category", "Value": "Books"}, {"Key": "published", "Value": "2019"}]
    })

    assert "SELECT \"$path\" " \
           "FROM \"amazonreviews\".\"amazon_reviews_parquet\" " \
           "WHERE (\"customer_id\" in ('123456', '456789')) " \
           "AND \"product_category\" = 'Books' " \
           "AND \"published\" = '2019'" == re.sub("[\x00-\x20]+", " ", resp.strip())


def test_it_generates_query_without_partition():
    resp = make_query({
        "Database": "amazonreviews",
        "Table": "amazon_reviews_parquet",
        "Columns": [{"Column": "customer_id", "MatchIds": ["123456", "456789"]}]
    })

    assert "SELECT \"$path\" " \
           "FROM \"amazonreviews\".\"amazon_reviews_parquet\" " \
           "WHERE (\"customer_id\" in ('123456', '456789'))" == re.sub("[\x00-\x20]+", " ", resp.strip())


def test_it_generates_query_with_multiple_columns():
    resp = make_query({
        "Database": "amazonreviews",
        "Table": "amazon_reviews_parquet",
        "Columns": [
            {"Column": "a", "MatchIds": ["a123456", "b123456"]},
            {"Column": "b", "MatchIds": ["a456789", "b456789"]},
        ]
    })

    assert "SELECT \"$path\" " \
           "FROM \"amazonreviews\".\"amazon_reviews_parquet\" " \
           "WHERE (\"a\" in ('a123456', 'b123456') OR \"b\" in ('a456789', 'b456789'))" == re.sub("[\x00-\x20]+", " ",
                                                                                                  resp.strip())


def test_it_escapes_strings():
    assert "''' OR 1=1'" == escape_item("' OR 1=1")


def test_it_escapes_ints():
    assert 2 == escape_item(2)


def test_it_escapes_floats():
    assert float(2) == escape_item(float(2))


def test_it_escapes_none():
    assert 'NULL' == escape_item(None)


def test_it_raises_for_unsupported_type():
    with pytest.raises(ValueError):
        escape_item(["val"])

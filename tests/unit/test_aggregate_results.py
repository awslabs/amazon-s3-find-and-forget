from types import SimpleNamespace

import pytest

from lambdas.src.tasks.aggregate_results import handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


def test_it_deduplicates_paths():
    resp = handler([
        {
            "Columns": [{"Column": "customer_id", "MatchIds": ["abc", "def"]}],
            "Paths": [
                "s3://mybucket/parquet/product_category=Books/part1.snappy.parquet",
            ]
        },
        {
            "Columns": [{"Column": "customer_id", "MatchIds": ["ghi", "jkl"]}],
            "Paths": [
                "s3://mybucket/parquet/product_category=CrimeBooks/part1.snappy.parquet",
                "s3://mybucket/parquet/product_category=Books/part1.snappy.parquet",
            ]
        }
    ], SimpleNamespace())

    assert [
        {
            "Object": "s3://mybucket/parquet/product_category=Books/part1.snappy.parquet",
            "Columns": [
                {"Column": "customer_id", "MatchIds": ["abc", "def", "ghi", "jkl"]}
            ]
        },
        {
            "Object": "s3://mybucket/parquet/product_category=CrimeBooks/part1.snappy.parquet",
            "Columns": [
                {"Column": "customer_id", "MatchIds": ["ghi", "jkl"]}
            ]
        }
    ] == resp


def test_it_concats_multiple_columns_for_same_object():
    resp = handler([
        {
            "Columns": [{"Column": "customer_id", "MatchIds": ["abc", "def"]}],
            "Paths": [
                "s3://mybucket/parquet/product_category=Books/part1.snappy.parquet",
            ]
        },
        {
            "Columns": [{"Column": "email", "MatchIds": ["ghi", "jkl"]}],
            "Paths": [
                "s3://mybucket/parquet/product_category=Books/part1.snappy.parquet",
            ]
        }
    ], SimpleNamespace())

    assert [
        {
            "Object": "s3://mybucket/parquet/product_category=Books/part1.snappy.parquet",
            "Columns": [
                {"Column": "customer_id", "MatchIds": ["abc", "def"]},
                {"Column": "email", "MatchIds": ["ghi", "jkl"]}
            ]
        }
    ] == resp


def test_it_deduplicates_matches_for_same_column():
    resp = handler([
        {
            "Columns": [{"Column": "customer_id", "MatchIds": ["abc", "def"]}],
            "Paths": [
                "s3://mybucket/parquet/product_category=Books/part1.snappy.parquet",
            ]
        },
        {
            "Columns": [{"Column": "customer_id", "MatchIds": ["abc", "fgh"]}],
            "Paths": [
                "s3://mybucket/parquet/product_category=CrimeBooks/part1.snappy.parquet",
                "s3://mybucket/parquet/product_category=Books/part1.snappy.parquet",
            ]
        }
    ], SimpleNamespace())

    assert [
        {
            "Object": "s3://mybucket/parquet/product_category=Books/part1.snappy.parquet",
            "Columns": [
                {"Column": "customer_id", "MatchIds": ["abc", "def", "fgh"]}
            ]
        },
        {
            "Object": "s3://mybucket/parquet/product_category=CrimeBooks/part1.snappy.parquet",
            "Columns": [
                {"Column": "customer_id", "MatchIds": ["abc", "fgh"]}
            ]
        }
    ] == resp

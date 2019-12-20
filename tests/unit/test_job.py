import datetime
import decimal
import json
from types import SimpleNamespace

import pytest
from mock import patch, ANY

from backend.lambdas.jobs import handlers

pytestmark = [pytest.mark.unit, pytest.mark.api, pytest.mark.jobs]


@patch("backend.lambdas.jobs.handlers.table")
def test_it_retrieves_jobs(table):
    mock_job = {"JobId": "test"}
    table.get_item.return_value = {
        "Item": mock_job
    }
    response = handlers.get_job_handler({
        "pathParameters": {"job_id": "test"}
    }, SimpleNamespace())

    assert 200 == response["statusCode"]
    assert mock_job == json.loads(response["body"])
    assert ANY == response["headers"]


@patch("backend.lambdas.jobs.handlers.table")
def test_it_retrieves_returns_job_not_found(table):
    table.get_item.return_value = {}
    response = handlers.get_job_handler({
        "pathParameters": {"job_id": "test"}
    }, SimpleNamespace())

    assert 404 == response["statusCode"]
    assert ANY == response["headers"]


@patch("backend.lambdas.jobs.handlers.table")
def test_it_lists_jobs(table):
    stub = job_stub()
    table.query.return_value = {"Items": [stub]}
    response = handlers.list_jobs_handler({}, SimpleNamespace())
    resp_body = json.loads(response["body"])
    assert 200 == response["statusCode"]
    assert len(resp_body["Jobs"])
    assert stub == resp_body["Jobs"][0]


@patch("backend.lambdas.jobs.handlers.bucket_count", 3)
@patch("backend.lambdas.jobs.handlers.table")
def test_it_queries_all_gsi_buckets(table):
    stub = job_stub()
    table.query.return_value = {"Items": [stub]}
    handlers.list_jobs_handler({}, SimpleNamespace())
    assert 3 == table.query.call_count


@patch("backend.lambdas.jobs.handlers.Key")
@patch("backend.lambdas.jobs.handlers.table")
def test_it_handles_start_at_qs(table, k):
    stub = job_stub()
    table.query.return_value = {"Items": [stub]}
    handlers.list_jobs_handler({"queryStringParameters": {"start_at": "12345"}}, SimpleNamespace())
    k.assert_called_with('CreatedAt')
    k().lt.assert_called_with(12345)


@patch("backend.lambdas.jobs.handlers.table")
def test_it_respects_page_size(table):
    stub = job_stub()
    table.query.return_value = {"Items": [stub for _ in range(0, 3)]}
    handlers.list_jobs_handler({"queryStringParameters": {"page_size": 3}}, SimpleNamespace())
    table.query.assert_called_with(
        IndexName=ANY,
        KeyConditionExpression=ANY,
        ScanIndexForward=ANY,
        Limit=3,
    )


@patch("backend.lambdas.jobs.handlers.bucket_count", 3)
@patch("backend.lambdas.jobs.handlers.table")
def test_it_respects_page_size_with_multiple_buckets(table):
    table.query.return_value = {"Items": [job_stub() for _ in range(0, 5)]}
    resp = handlers.list_jobs_handler({"queryStringParameters": {"page_size": 5}}, SimpleNamespace())
    assert 3 == table.query.call_count
    assert 5 == len(json.loads(resp["body"])["Jobs"])


def test_decimal_encoder():
    res_a = json.dumps({"k": decimal.Decimal(1.1)}, cls=handlers.DecimalEncoder)
    res_b = json.dumps({"k": decimal.Decimal(1.5)}, cls=handlers.DecimalEncoder)
    assert "{\"k\": 1}" == res_a
    assert "{\"k\": 2}" == res_b


def job_stub(job_id="test", created_at=round(datetime.datetime.now().timestamp())):
    return {"JobId": job_id, "CreatedAt": created_at}
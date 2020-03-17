import datetime
import decimal
import json
from types import SimpleNamespace

import mock
import pytest
from botocore.exceptions import ClientError
from mock import patch, ANY

from backend.lambdas.jobs import handlers

pytestmark = [pytest.mark.unit, pytest.mark.api, pytest.mark.jobs]


@patch("backend.lambdas.jobs.handlers.table")
def test_it_retrieves_jobs(table):
    mock_job = {"Id": "test"}
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
    assert 1 == len(resp_body["Jobs"])
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
def test_it_handles_list_job_start_at_qs(table, k):
    stub = job_stub()
    table.query.return_value = {"Items": [stub]}
    handlers.list_jobs_handler({"queryStringParameters": {"start_at": "12345"}}, SimpleNamespace())
    k.assert_called_with("CreatedAt")
    k().lt.assert_called_with(12345)


@patch("backend.lambdas.jobs.handlers.table")
def test_it_respects_list_job_page_size(table):
    stub = job_stub()
    table.query.return_value = {"Items": [stub for _ in range(0, 3)]}
    handlers.list_jobs_handler({"queryStringParameters": {"page_size": "3"}}, SimpleNamespace())
    table.query.assert_called_with(
        IndexName=ANY,
        KeyConditionExpression=ANY,
        ScanIndexForward=ANY,
        Limit=3,
    )


def test_it_rejects_invalid_page_size_for_list_jobs():
    response = handlers.list_jobs_handler({"queryStringParameters": {"page_size": "NaN"}}, SimpleNamespace())
    assert 422 == response["statusCode"]


def test_it_rejects_invalid_start_at_for_list_jobs():
    response = handlers.list_jobs_handler({"queryStringParameters": {"start_at": "badformat"}}, SimpleNamespace())
    assert 422 == response["statusCode"]


def test_it_rejects_invalid_page_size_for_list_job_events():
    response = handlers.list_jobs_handler({"queryStringParameters": {"page_size": "NaN"}}, SimpleNamespace())
    assert 422 == response["statusCode"]


@patch("backend.lambdas.jobs.handlers.table")
def test_it_rejects_invalid_start_at_for_list_job_events(table):
    stub = job_stub()
    table.query.return_value = {"Items": [stub for _ in range(0, 3)]}
    response = handlers.list_jobs_handler({"queryStringParameters": {"page_size": "badformat"}}, SimpleNamespace())
    assert 422 == response["statusCode"]


@patch("backend.lambdas.jobs.handlers.bucket_count", 3)
@patch("backend.lambdas.jobs.handlers.table")
def test_it_respects_list_job_page_size_with_multiple_buckets(table):
    table.query.return_value = {"Items": [job_stub() for _ in range(0, 5)]}
    resp = handlers.list_jobs_handler({"queryStringParameters": {"page_size": "5"}}, SimpleNamespace())
    assert 3 == table.query.call_count
    assert 5 == len(json.loads(resp["body"])["Jobs"])


@patch("backend.lambdas.jobs.handlers.table")
def test_it_lists_jobs_events(table):
    stub = job_event_stub()
    table.get_item.return_value = job_stub()
    table.query.return_value = {"Items": [stub]}
    response = handlers.list_job_events_handler({"pathParameters": {"job_id": "test"}}, SimpleNamespace())
    resp_body = json.loads(response["body"])
    assert 200 == response["statusCode"]
    assert 1 == len(resp_body["JobEvents"])
    assert stub == resp_body["JobEvents"][0]


@patch("backend.lambdas.jobs.handlers.table")
def test_it_respects_jobs_events_page_size(table):
    table.get_item.return_value = job_stub()
    table.query.return_value = {
        "Items": [
            job_event_stub(job_id="job123", sk=str(i)) for i in range(1, 5)
        ],
        "LastEvaluatedKey": {"Id": "test", "Sk": "12345"}
    }
    response = handlers.list_job_events_handler({
        "pathParameters": {"job_id": "test"},
        "queryStringParameters": {"page_size": "3"},
    }, SimpleNamespace())
    resp_body = json.loads(response["body"])
    table.query.assert_called_with(
        KeyConditionExpression=mock.ANY,
        ScanIndexForward=True,
        Limit=4,
        FilterExpression=mock.ANY,
        ExclusiveStartKey=mock.ANY,
    )
    assert 200 == response["statusCode"]
    assert 3 == len(resp_body["JobEvents"])
    assert "3" == resp_body["JobEvents"][len(resp_body["JobEvents"]) - 1]["Sk"]
    assert "NextStart" in resp_body


@patch("backend.lambdas.jobs.handlers.table")
def test_it_starts_at_earliest_by_default(table):
    stub = job_event_stub()
    table.get_item.return_value = job_stub()
    table.query.return_value = {"Items": [stub]}
    response = handlers.list_job_events_handler({"pathParameters": {"job_id": "test"}}, SimpleNamespace())
    assert 200 == response["statusCode"]
    table.query.assert_called_with(
        KeyConditionExpression=mock.ANY,
        ScanIndexForward=True,
        Limit=mock.ANY,
        ExclusiveStartKey={
            "Id": "test",
            "Sk": "0"
        },
        FilterExpression=mock.ANY,
    )


@patch("backend.lambdas.jobs.handlers.table")
def test_it_starts_at_supplied_watermark(table):
    stub = job_event_stub()
    table.get_item.return_value = job_stub()
    table.query.return_value = {"Items": [stub], "LastEvaluatedKey": {"Id": "test", "Sk": "12345#test"}}
    response = handlers.list_job_events_handler({
        "pathParameters": {"job_id": "test"},
        "queryStringParameters": {"start_at": "12345#test"},
    }, SimpleNamespace())
    resp_body = json.loads(response["body"])
    assert 200 == response["statusCode"]
    assert "NextStart" in resp_body
    table.query.assert_called_with(
        KeyConditionExpression=mock.ANY,
        ScanIndexForward=mock.ANY,
        Limit=mock.ANY,
        FilterExpression=mock.ANY,
        ExclusiveStartKey={
            "Id": "test",
            "Sk": "12345#test"
        },
    )


@patch("backend.lambdas.jobs.handlers.table")
def test_it_returns_provided_watermark_for_no_events_for_incomplete_job(table):
    table.get_item.return_value = job_stub()
    table.query.return_value = {"Items": []}
    response = handlers.list_job_events_handler({
        "pathParameters": {"job_id": "test"},
        "queryStringParameters": {"start_at": "111111#trgwtrwgergewrgwgrw"},
    }, SimpleNamespace())
    resp_body = json.loads(response["body"])
    assert 200 == response["statusCode"]
    assert "111111#trgwtrwgergewrgwgrw" == resp_body["NextStart"]


@patch("backend.lambdas.jobs.handlers.table")
def test_it_returns_watermark_where_not_last_page_and_job_complete(table):
    table.get_item.return_value = job_stub(JobFinishTime=12345)
    table.query.return_value = {"Items": [job_event_stub(Sk=str(i)) for i in range(0, 20)]}
    response = handlers.list_job_events_handler({
        "pathParameters": {"job_id": "test"},
    }, SimpleNamespace())
    resp_body = json.loads(response["body"])
    assert 200 == response["statusCode"]
    assert "19" == resp_body["NextStart"]


@patch("backend.lambdas.jobs.handlers.table")
def test_it_does_not_return_watermark_if_last_page_reached(table):
    events = ["CleanupSucceeded", "CleanupFailed", "CleanupSkipped"]
    for e in events:
        stub = job_event_stub(EventName=e)
        table.get_item.return_value = job_stub()
        table.query.return_value = {"Items": [stub]}
        response = handlers.list_job_events_handler({
            "pathParameters": {"job_id": "test"},
            "queryStringParameters": {"page_size": "1"}
        }, SimpleNamespace())
        resp_body = json.loads(response["body"])
        assert 200 == response["statusCode"]
        assert "NextStart" not in resp_body


@patch("backend.lambdas.jobs.handlers.table")
def test_it_errors_if_job_not_found(table):
    table.get_item.side_effect = ClientError({"ResponseMetadata": {"HTTPStatusCode": 404}}, "get_item")
    response = handlers.list_job_events_handler({
        "pathParameters": {"job_id": "test"},
        "queryStringParameters": {"start_at": "12345#test"},
    }, SimpleNamespace())
    assert 404 == response["statusCode"]


@patch("backend.lambdas.jobs.handlers.table")
def test_it_returns_error_if_invalid_watermark_supplied_for_completed_job(table):
    table.get_item.return_value = job_stub(JobFinishTime=12345)
    response = handlers.list_job_events_handler({
        "pathParameters": {"job_id": "test"},
        "queryStringParameters": {"start_at": "999999999999999#test"},
    }, SimpleNamespace())
    assert 400 == response["statusCode"]


@patch("backend.lambdas.jobs.handlers.table")
def test_it_returns_error_if_invalid_watermark_supplied_for_running_job(table):
    table.get_item.return_value = job_stub()
    response = handlers.list_job_events_handler({
        "pathParameters": {"job_id": "test"},
        "queryStringParameters": {"start_at": "999999999999999#test"},
    }, SimpleNamespace())
    assert 400 == response["statusCode"]


def job_stub(job_id="test", created_at=round(datetime.datetime.utcnow().timestamp()), **kwargs):
    return {"Id": job_id, "Sk": job_id, "CreatedAt": created_at, "Type": "Job", **kwargs}


def job_event_stub(job_id="test", sk=None, **kwargs):
    now = round(datetime.datetime.utcnow().timestamp())
    if not sk:
        sk = "{}#{}".format(str(now), "12345")
    return {"Id": job_id, "Sk": sk, "Type": "JobEvent", "CreatedAt": now, "EventName": "QuerySucceeded", **kwargs}

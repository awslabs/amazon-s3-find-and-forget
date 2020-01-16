import json
import os
from types import SimpleNamespace

import mock
import pytest
from mock import patch

with patch.dict(os.environ, {"QueryQueue": "test"}):
    from backend.lambdas.tasks.generate_report import handler, write_log, get_status, get_aggregated_query_stats, \
    get_aggregated_object_stats, get_job_logs, write_summary, convert_iso8601_to_epoch, normalise_dates

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.tasks.generate_report.normalise_dates")
@patch("backend.lambdas.tasks.generate_report.write_summary")
@patch("backend.lambdas.tasks.generate_report.write_log")
@patch("backend.lambdas.tasks.generate_report.get_job_logs")
@patch("backend.lambdas.tasks.generate_report.get_aggregated_query_stats")
@patch("backend.lambdas.tasks.generate_report.get_aggregated_object_stats")
@patch("backend.lambdas.tasks.generate_report.get_status")
def test_it_generates_reports(mock_get_status, mock_query_stats, mock_object_stats, mock_get_logs, mock_write_log,
                              mock_write_summary, mock_normalise_dates):
    mock_get_status.return_value = "COMPLETED"
    mock_query_stats.return_value = {
        "TotalQueryTimeInMillis": 10000,
        "TotalQueryScannedInBytes": 1000,
        "TotalQueryCount": 10,
        "TotalQueryFailedCount": 0,
    }
    mock_object_stats.return_value = {
       "TotalObjectUpdatedCount": 1,
       "TotalObjectUpdateFailedCount": 0,
    }
    mock_get_logs.return_value = [
        {"message": json.dumps({"EventName": "QuerySucceeded", "EventData": query_stub()})},
        {"message": json.dumps({"EventName": "ObjectUpdated", "EventData": object_update_stub()})},
    ]
    mock_write_log.return_value = "s3://some_bucket/reports/123.json"
    mock_normalise_dates.return_value = {
        'JobId': '123',
        'JobStartTime': 1578327177,
        'JobFinishTime': 1578327177,
        'CreatedAt': 123456,
        'GSIBucket': '0',
        'QuerySucceeded': [{'QueryStatus': {'Statistics': {'DataScannedInBytes': 100, 'EngineExecutionTimeInMillis': 1000}}}],
        'ObjectUpdated': [{}],
        'TotalObjectUpdatedCount': 1,
        'TotalObjectUpdateFailedCount': 0,
        'TotalQueryTimeInMillis': 10000,
        'TotalQueryScannedInBytes': 1000,
        'TotalQueryCount': 10,
        'TotalQueryFailedCount': 0,
        'JobStatus': 'COMPLETED'
    }
    resp = handler({
        "Bucket": "some_bucket",
        "JobStartTime": "2019-12-05T13:38:02.858Z",
        "JobFinishTime": "2019-12-05T13:39:37.220Z",
        "JobId": "123",
        "Input": {
            "GSIBucket": "0",
            "CreatedAt": 123456,
        }
    }, SimpleNamespace())
    mock_write_log.assert_called_with("some_bucket", "123", mock.ANY)
    mock_write_summary.assert_called()
    mock_normalise_dates.assert_called_with({
        'JobId': '123',
        'JobStartTime': '2019-12-05T13:38:02.858Z',
        'JobFinishTime': '2019-12-05T13:39:37.220Z',
        'CreatedAt': 123456,
        'GSIBucket': '0',
        'QuerySucceeded': [{'QueryStatus': {'Statistics': {'DataScannedInBytes': 100, 'EngineExecutionTimeInMillis': 1000}}}],
        'ObjectUpdated': [{}],
        'TotalObjectUpdatedCount': 1,
        'TotalObjectUpdateFailedCount': 0,
        'TotalQueryTimeInMillis': 10000,
        'TotalQueryScannedInBytes': 1000,
        'TotalQueryCount': 10,
        'TotalQueryFailedCount': 0,
        'JobStatus': 'COMPLETED'
    })
    assert {
        "JobId": "123",
        "JobStartTime": 1578327177,
        "JobFinishTime": 1578327177,
        "GSIBucket": "0",
        "CreatedAt": 123456,
        "TotalObjectUpdatedCount": 1,
        "TotalObjectUpdateFailedCount": 0,
        "TotalQueryTimeInMillis": 10000,
        "TotalQueryScannedInBytes": 1000,
        "TotalQueryCount": 10,
        "TotalQueryFailedCount": 0,
        "JobStatus": "COMPLETED",
        "JobReportLocation": "s3://some_bucket/reports/123.json"
    } == resp


@patch("backend.lambdas.tasks.generate_report.s3")
def test_it_writes_log_to_s3(mock_s3):
    mock_object = mock.MagicMock()
    mock_s3.Object.return_value = mock_object
    expected = report_stub(JobId="1234")
    report_location = write_log("test_bucket", "1234", expected)
    assert 1 == mock_s3.Object.call_count
    report = json.loads(mock_object.put.call_args_list[0][1]["Body"])
    assert expected == report
    assert report_location == "s3://test_bucket/reports/1234.json"


@patch("backend.lambdas.tasks.generate_report.table")
def test_it_writes_summary(mock_table):
    write_summary({"report": "data"})
    mock_table.put_item.assert_called_with(Item={"report": "data"})


@patch("backend.lambdas.tasks.generate_report.logs")
def test_it_gets_all_logs(mock_client):
    mock_client.filter_log_events.side_effect = [
        {"events": ["log1"], "nextToken": 123}, {"events": ["log2"]}
    ]

    resp = list(get_job_logs("job_id"))
    assert [
        "log1", "log2"
    ] == resp
    assert 2 == mock_client.filter_log_events.call_count


def test_it_detects_success():
    resp = get_status({
        "QuerySucceeded": [query_stub()],
        "ObjectUpdated": [object_update_stub()]
    })
    assert "COMPLETED" == resp


def test_it_detects_query_fail():
    resp = get_status({
        "QuerySucceeded": [query_stub()],
        "QueryFailed": [query_stub()],
    })
    assert "ABORTED" == resp


def test_it_detects_write_fail():
    resp = get_status({
        "QuerySucceeded": [query_stub()],
        "ObjectUpdate": [object_update_stub()],
        "ObjectUpdateFailed": [object_update_stub()],
    })
    assert "COMPLETED_WITH_ERRORS" == resp


def test_it_detects_generic_fails():
    resp = get_status({
        "Exception": [{}],
    })
    assert "FAILED" == resp


def test_it_gets_aggregated_query_stats():
    resp = get_aggregated_query_stats({
        "QuerySucceeded": [query_stub() for _ in range(0, 5)],
        "QueryFailed": [query_stub() for _ in range(0, 5)],
    })
    assert {
        "TotalQueryTimeInMillis": 10000,
        "TotalQueryScannedInBytes": 1000,
        "TotalQueryCount": 10,
        "TotalQueryFailedCount": 5,
    } == resp


def test_it_gets_handles_no_stats():
    resp = get_aggregated_query_stats({
        "QuerySucceeded": [query_stub() for _ in range(0, 5)],
        "QueryFailed": [{} for _ in range(0, 5)],
    })
    assert {
        "TotalQueryTimeInMillis": 5000,
        "TotalQueryScannedInBytes": 500,
        "TotalQueryCount": 10,
        "TotalQueryFailedCount": 5,
    } == resp


def test_it_gets_aggregated_object_update_stats():
    resp = get_aggregated_object_stats({
        "ObjectUpdated": [object_update_stub()],
        "ObjectUpdateFailed": [object_update_stub()],
    })
    assert {
       "TotalObjectUpdatedCount": 1,
       "TotalObjectUpdateFailedCount": 1,
    } == resp


def test_it_normalises_date_like_fields():
    assert {
        "a": [{"a": 1578348777, "b": "string"}],
        "b": [1578348777],
        "c": {"a": 1578348777},
        "d": 1578348777,
        "e": "string",
        "f": 2,
    } == normalise_dates({
        "a": [{"a": "2020-01-06T16:12:57.092Z", "b": "string"}],
        "b": ["2020-01-06T16:12:57.092Z"],
        "c": {"a": "2020-01-06T16:12:57.092Z"},
        "d": "2020-01-06T16:12:57.092Z",
        "e": "string",
        "f": 2,
    })


def test_it_converts_sfn_datetimes_to_epoch():
    assert 1578348777 == convert_iso8601_to_epoch("2020-01-06T16:12:57.092Z")


def query_stub(**kwargs):
    return {
        "QueryStatus": {
            "Statistics": {
                "DataScannedInBytes": 100,
                "EngineExecutionTimeInMillis": 1000,
            }
        },
        **kwargs,
    }


def object_update_stub(**kwargs):
    return {
        **kwargs
    }


def report_stub(**kwargs):
    return {
        "GSIBucket": "0",
        "CreatedAt": 123456,
        "JobStartTime": "2019-12-05T13:38:02.858Z",
        "JobFinishTime": "2019-12-05T13:39:37.220Z",
        "JobId": "28921fc6-17ca-4a1b-bc1a-ffaf1a5a4bae",
        "JobStatus": "COMPLETED",
        "TotalQueryTimeInMillis": 100,
        "TotalQueryScannedInBytes": 100,
        "TotalQueryCount": 10,
        "TotalObjectUpdatedCount": 5,
        "TotalQueryFailedCount": 0,
        "ObjectUpdated": [object_update_stub()],
        "QuerySucceeded": [query_stub()],
        **kwargs
    }


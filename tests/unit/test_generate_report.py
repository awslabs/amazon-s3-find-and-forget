import json
import os
from types import SimpleNamespace

import mock
import pytest
from mock import patch

with patch.dict(os.environ, {"QueryQueue": "test"}):
    from backend.lambdas.tasks.generate_report import handler, write_reports, get_status, get_aggregated_query_stats,\
        get_aggregated_object_stats, get_job_logs


pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.tasks.generate_report.write_reports")
@patch("backend.lambdas.tasks.generate_report.get_job_logs")
@patch("backend.lambdas.tasks.generate_report.get_aggregated_query_stats")
@patch("backend.lambdas.tasks.generate_report.get_aggregated_object_stats")
@patch("backend.lambdas.tasks.generate_report.get_status")
def test_it_generates_reports(mock_get_status, mock_query_stats, mock_object_stats, mock_get_logs, mock_write):
    mock_get_status.return_value = "COMPLETED"
    mock_query_stats.return_value = {
        "TotalQueryTimeInMillis": 10000,
        "TotalQueryScannedInBytes": 1000,
        "TotalQueryCount": 10,
    }
    mock_object_stats.return_value = {
       "TotalObjectUpdatedCount": 1,
       "TotalObjectUpdateFailedCount": 1,
    }
    mock_get_logs.return_value = [
        {"message": json.dumps({"EventName": "QuerySucceeded", "EventData": query_stub()})},
        {"message": json.dumps({"EventName": "ObjectUpdated", "EventData": object_update_stub()})},
    ]
    resp = handler({
        "Bucket": "some_bucket",
        "JobStartTime": "2019-12-05T13:38:02.858Z",
        "JobFinishTime": "2019-12-05T13:39:37.220Z",
        "JobId": "123"
    }, SimpleNamespace())
    mock_write.assert_called_with("some_bucket", "123", {
        "JobStartTime": "2019-12-05T13:38:02.858Z",
        "JobFinishTime": "2019-12-05T13:39:37.220Z",
        "JobId": "123",
        "QuerySucceeded": [query_stub()],
        "ObjectUpdated": [object_update_stub()],
        "TotalObjectUpdatedCount": 1,
        "TotalObjectUpdateFailedCount": 0,
        "TotalQueryFailedCount": 1,
        "TotalQueryTimeInMillis": 10000,
        "TotalQueryScannedInBytes": 1000,
        "TotalQueryCount": 10,
        "JobStatus": "COMPLETED"
    })
    assert [
       "reports/123/detailed.json",
       "reports/123/summary.json"
    ] == resp


@patch("backend.lambdas.tasks.generate_report.s3")
def test_it_generates_reports(mock_s3):
    mock_object = mock.MagicMock()
    mock_s3.Object.return_value = mock_object
    detailed = report_stub(JobId="123")
    summary = {k: v for k, v in detailed.items() if k not in ["ObjectUpdated", "QuerySucceeded"]}
    resp = write_reports("test_bucket", "123", detailed)
    assert [
        "reports/123/detailed.json",
        "reports/123/summary.json"
    ] == resp
    assert 2 == mock_s3.Object.call_count
    reports = [json.loads(arg[1]["Body"]) for arg in mock_object.put.call_args_list]
    assert summary in reports
    assert detailed in reports


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

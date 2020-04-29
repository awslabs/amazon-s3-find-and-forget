import datetime
import json
from types import SimpleNamespace

import mock
import pytest
from boto3.dynamodb.conditions import And, Attr
from botocore.exceptions import ClientError
from mock import patch, ANY

from backend.lambdas.jobs import handlers

pytestmark = [pytest.mark.unit, pytest.mark.api, pytest.mark.jobs]


class TestGetJob:
    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_retrieves_job(self, table):
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
    def test_it_returns_404_for_job_not_found(self, table):
        table.get_item.side_effect = ClientError({"ResponseMetadata": {"HTTPStatusCode": 404}}, "get_item")
        response = handlers.get_job_handler({
            "pathParameters": {"job_id": "test"}
        }, SimpleNamespace())

        assert 404 == response["statusCode"]
        assert ANY == response["headers"]


class TestListJobs:
    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_lists_jobs(self, table):
        stub = job_stub()
        table.query.return_value = {"Items": [stub]}
        response = handlers.list_jobs_handler({"queryStringParameters": None}, SimpleNamespace())
        resp_body = json.loads(response["body"])
        assert 200 == response["statusCode"]
        assert 1 == len(resp_body["Jobs"])
        assert stub == resp_body["Jobs"][0]

    @patch("backend.lambdas.jobs.handlers.bucket_count", 3)
    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_queries_all_gsi_buckets(self, table):
        stub = job_stub()
        table.query.return_value = {"Items": [stub]}
        handlers.list_jobs_handler({"queryStringParameters": None}, SimpleNamespace())
        assert 3 == table.query.call_count

    @patch("backend.lambdas.jobs.handlers.Key")
    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_handles_list_job_start_at_qs(self, table, k):
        stub = job_stub()
        table.query.return_value = {"Items": [stub]}
        handlers.list_jobs_handler({"queryStringParameters": {"start_at": "12345"}}, SimpleNamespace())
        k.assert_called_with("CreatedAt")
        k().lt.assert_called_with(12345)

    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_returns_requested_page_size_for_jobs(self, table):
        stub = job_stub()
        table.query.return_value = {"Items": [stub for _ in range(0, 3)]}
        handlers.list_jobs_handler({"queryStringParameters": {"page_size": "3"}}, SimpleNamespace())
        table.query.assert_called_with(
            IndexName=ANY,
            KeyConditionExpression=ANY,
            ScanIndexForward=ANY,
            Limit=3,
            ProjectionExpression=ANY
        )

    @patch("backend.lambdas.jobs.handlers.bucket_count", 3)
    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_returns_requested_page_size_for_jobs_with_multiple_gsi_buckets(self, table):
        table.query.return_value = {"Items": [job_stub() for _ in range(0, 5)]}
        resp = handlers.list_jobs_handler({"queryStringParameters": {"page_size": "5"}}, SimpleNamespace())
        assert 3 == table.query.call_count
        assert 5 == len(json.loads(resp["body"])["Jobs"])

    def test_it_rejects_invalid_page_size_for_list_jobs(self):
        response = handlers.list_jobs_handler({"queryStringParameters": {"page_size": "NaN"}}, SimpleNamespace())
        assert 422 == response["statusCode"]
        response = handlers.list_jobs_handler({"queryStringParameters": {"page_size": "0"}}, SimpleNamespace())
        assert 422 == response["statusCode"]
        response = handlers.list_jobs_handler({"queryStringParameters": {"page_size": "1001"}}, SimpleNamespace())
        assert 422 == response["statusCode"]

    def test_it_rejects_invalid_start_at_for_list_jobs(self):
        response = handlers.list_jobs_handler({"queryStringParameters": {"start_at": "badformat"}}, SimpleNamespace())
        assert 422 == response["statusCode"]

    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_rejects_invalid_start_at_for_list_jobs(self, table):
        stub = job_stub()
        table.query.return_value = {"Items": [stub for _ in range(0, 3)]}
        response = handlers.list_jobs_handler({"queryStringParameters": {"page_size": "badformat"}}, SimpleNamespace())
        assert 422 == response["statusCode"]


class TestListJobEvents:
    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_lists_jobs_events(self, table):
        stub = job_event_stub()
        table.get_item.return_value = {"Item": job_stub()}
        table.query.return_value = {"Items": [stub]}
        response = handlers.list_job_events_handler({
            "queryStringParameters": None,
            "pathParameters": {"job_id": "test"},
            "multiValueQueryStringParameters": None,
        }, SimpleNamespace())
        resp_body = json.loads(response["body"])
        assert 200 == response["statusCode"]
        assert 1 == len(resp_body["JobEvents"])
        assert stub == resp_body["JobEvents"][0]

    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_errors_if_job_not_found(self, table):
        table.get_item.side_effect = ClientError({"ResponseMetadata": {"HTTPStatusCode": 404}}, "get_item")
        response = handlers.list_job_events_handler({
            "pathParameters": {"job_id": "test"},
            "queryStringParameters": {"start_at": "12345#test"},
            "multiValueQueryStringParameters": {},
        }, SimpleNamespace())
        assert 404 == response["statusCode"]

    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_returns_requested_page_size_for_jobs_events(self, table):
        table.get_item.return_value = {"Item": job_stub()}
        table.query.return_value = {
            "Items": [
                job_event_stub(job_id="job123", sk=str(i)) for i in range(1, 5)
            ],
            "LastEvaluatedKey": {"Id": "test", "Sk": "12345"}
        }
        response = handlers.list_job_events_handler({
            "pathParameters": {"job_id": "test"},
            "queryStringParameters": {"page_size": "3"},
            "multiValueQueryStringParameters": {"page_size": ["3"]},
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
    def test_it_starts_at_earliest_by_default(self, table):
        stub = job_event_stub()
        table.get_item.return_value = {"Item": job_stub()}
        table.query.return_value = {"Items": [stub]}
        response = handlers.list_job_events_handler({
            "queryStringParameters": None,
            "pathParameters": {"job_id": "test"},
            "multiValueQueryStringParameters": None,
        }, SimpleNamespace())
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
    def test_it_accepts_start_at_earliest_watermark(self, table):
        stub = job_event_stub()
        table.get_item.return_value = {"Item": job_stub()}
        table.query.return_value = {"Items": [stub], "LastEvaluatedKey": {"Id": "test", "Sk": "12345#test"}}
        response = handlers.list_job_events_handler({
            "pathParameters": {"job_id": "test"},
            "queryStringParameters": {"start_at": "0"},
            "multiValueQueryStringParameters": {"start_at": ["0"]},
        }, SimpleNamespace())
        resp_body = json.loads(response["body"])
        assert 200 == response["statusCode"]
        assert "NextStart" in resp_body
        assert {
                   "KeyConditionExpression": mock.ANY,
                   "ScanIndexForward": mock.ANY,
                   "Limit": mock.ANY,
                   "FilterExpression": mock.ANY,
                   "ExclusiveStartKey": {
                       "Id": "test",
                       "Sk": "0"
                   }
               } == table.query.call_args_list[0][1]

    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_starts_at_watermark(self, table):
        stub = job_event_stub()
        table.get_item.return_value = {"Item": job_stub()}
        table.query.return_value = {"Items": [stub], "LastEvaluatedKey": {"Id": "test", "Sk": "12345#test"}}
        response = handlers.list_job_events_handler({
            "pathParameters": {"job_id": "test"},
            "queryStringParameters": {"start_at": "12345#test"},
            "multiValueQueryStringParameters": {"start_at": ["12345#test"]},
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
    def test_it_handles_watermark_with_microseconds_in_same_second(self, table):
        stub = job_event_stub()
        table.get_item.return_value = {"Item": job_stub(JobFinishTime=12345)}
        table.query.return_value = {"Items": [stub]}
        response = handlers.list_job_events_handler({
            "pathParameters": {"job_id": "test"},
            "queryStringParameters": {"start_at": "12345001#test"},
            "multiValueQueryStringParameters": {"start_at": ["12345001#test"]},
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
                "Sk": "12345001#test"
            },
        )

    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_returns_page_size_where_job_item_in_initial_query_response(self, table):
        job = job_stub(JobFinishTime=12345, JobStatus="COMPLETED")
        table.get_item.return_value = {"Item": job}
        table.query.side_effect = [
            {"Items": [job_event_stub(Sk=str(i)) for i in range(0, 19)],
             "LastEvaluatedKey": {"Id": job["Id"], "Sk": "19"}},
            {"Items": [job_event_stub(Sk="20")]},
        ]
        response = handlers.list_job_events_handler({
            "queryStringParameters": None,
            "pathParameters": {"job_id": "test"},
            "multiValueQueryStringParameters": None,
        }, SimpleNamespace())
        resp_body = json.loads(response["body"])
        assert 2 == table.query.call_count
        assert 200 == response["statusCode"]
        assert 20 == len(resp_body["JobEvents"])
        assert "NextStart" not in resp_body

    # Running jobs
    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_returns_last_item_watermark_for_incomplete_job(self, table):
        stub = job_event_stub()
        table.get_item.return_value = {"Item": job_stub(JobStatus="RUNNING")}
        table.query.return_value = {"Items": [stub]}
        response = handlers.list_job_events_handler({
            "pathParameters": {"job_id": "test"},
        }, SimpleNamespace())
        resp_body = json.loads(response["body"])
        assert 200 == response["statusCode"]
        assert stub["Sk"] == resp_body["NextStart"]

    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_returns_last_item_watermark_for_incomplete_job_where_page_size_is_fulfilled(self, table):
        stub = job_event_stub()
        table.get_item.return_value = {"Item": job_stub(JobStatus="RUNNING")}
        table.query.return_value = {"Items": [stub]}
        response = handlers.list_job_events_handler({
            "pathParameters": {"job_id": "test"},
            "queryStringParameters": {"page_size": "1"},
            "multiValueQueryStringParameters": {"page_size": ["1"]},
        }, SimpleNamespace())
        resp_body = json.loads(response["body"])
        assert 200 == response["statusCode"]
        assert stub["Sk"] == resp_body["NextStart"]

    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_returns_provided_watermark_for_no_events_for_incomplete_job(self, table):
        table.get_item.return_value = {"Item": job_stub(JobStatus="RUNNING")}
        table.query.return_value = {"Items": []}
        response = handlers.list_job_events_handler({
            "pathParameters": {"job_id": "test"},
            "queryStringParameters": {"start_at": "111111#trgwtrwgergewrgwgrw"},
            "multiValueQueryStringParameters": {"start_at": ["111111#trgwtrwgergewrgwgrw"]},
        }, SimpleNamespace())
        resp_body = json.loads(response["body"])
        assert 200 == response["statusCode"]
        assert "111111#trgwtrwgergewrgwgrw" == resp_body["NextStart"]

    # Completed jobs
    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_returns_last_item_watermark_where_not_last_page_and_job_complete(self, table):
        job = job_stub(JobFinishTime=12345, JobStatus="COMPLETED")
        table.get_item.return_value = {"Item": job}
        table.query.return_value = {"Items": [job_event_stub(Sk=str(i)) for i in range(0, 20)],
                                    "LastEvaluatedKey": {"Id": job["Id"], "Sk": "19"}}
        response = handlers.list_job_events_handler({
            "queryStringParameters": None,
            "pathParameters": {"job_id": "test"},
            "multiValueQueryStringParameters": None,
        }, SimpleNamespace())
        resp_body = json.loads(response["body"])
        assert 200 == response["statusCode"]
        assert "19" == resp_body["NextStart"]

    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_does_not_return_watermark_if_last_page_reached_on_complete_job(self, table):
        events = [
            "COMPLETED_CLEANUP_FAILED",
            "COMPLETED",
            "FAILED",
            "FIND_FAILED",
            "FORGET_FAILED",
            "FORGET_PARTIALLY_FAILED"
        ]
        for s in events:
            event_stub = job_event_stub()
            table.get_item.return_value = {"Item": job_stub(JobStatus=s)}
            table.query.return_value = {"Items": [event_stub]}
            response = handlers.list_job_events_handler({
                "pathParameters": {"job_id": "test"},
                "queryStringParameters": None,
                "multiValueQueryStringParameters": None,
            }, SimpleNamespace())
            resp_body = json.loads(response["body"])
            assert 200 == response["statusCode"]
            assert "NextStart" not in resp_body

    # Errors
    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_returns_error_if_invalid_watermark_supplied_for_completed_job(self, table):
        stub = job_event_stub()
        table.get_item.return_value = {"Item": job_stub(JobFinishTime=12345)}
        table.query.return_value = {"Items": [stub]}
        response = handlers.list_job_events_handler({
            "pathParameters": {"job_id": "test"},
            "queryStringParameters": {"start_at": "12346001#test"},
            "multiValueQueryStringParameters": {"start_at": ["12346001#test"]},
        }, SimpleNamespace())
        assert 400 == response["statusCode"]

    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_returns_error_if_invalid_watermark_supplied_for_running_job(self, table):
        table.get_item.return_value = {"Item": job_stub()}
        response = handlers.list_job_events_handler({
            "pathParameters": {"job_id": "test"},
            "queryStringParameters": {"start_at": "999999999999999#test"},
            "multiValueQueryStringParameters": {},
        }, SimpleNamespace())
        assert 400 == response["statusCode"]

    def test_it_rejects_invalid_page_size_for_list_job_events(self):
        response = handlers.list_job_events_handler({"queryStringParameters": {"page_size": "NaN"}}, SimpleNamespace())
        assert 422 == response["statusCode"]
        response = handlers.list_job_events_handler({"queryStringParameters": {"page_size": "0"}}, SimpleNamespace())
        assert 422 == response["statusCode"]
        response = handlers.list_job_events_handler({"queryStringParameters": {"page_size": "1001"}}, SimpleNamespace())
        assert 422 == response["statusCode"]


class TestListJobEventFilters:
    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_applies_filters(self, table):
        stub = job_event_stub()
        table.get_item.return_value = {"Item": job_stub()}
        table.query.return_value = {"Items": [stub]}
        response = handlers.list_job_events_handler({
            "pathParameters": {"job_id": "test"},
            "queryStringParameters": {"start_at": "0"},
            "multiValueQueryStringParameters": {"filter": ["EventName=QuerySucceeded"]}
        }, SimpleNamespace())
        resp_body = json.loads(response["body"])
        assert 200 == response["statusCode"]
        assert "NextStart" in resp_body
        assert 1 == table.query.call_count
        assert 1 == len(resp_body["JobEvents"])
        # Filter expression should be an And condition with 2 components
        # get_expression()["values"] returns the filters being applied as part of the And condition
        filter_expression = table.query.call_args_list[0][1]["FilterExpression"]
        assert isinstance(table.query.call_args_list[0][1]["FilterExpression"], And)
        assert Attr("Type").eq("JobEvent") in filter_expression.get_expression()["values"]
        assert Attr("EventName").begins_with("QuerySucceeded") in filter_expression.get_expression()["values"]
        assert 2 == len(filter_expression.get_expression()["values"])

    # Running jobs
    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_returns_ddb_watermark_where_ddb_response_is_less_than_page_size(self, table):
        job = job_stub(JobStatus="RUNNING")
        table.get_item.return_value = {"Item": job}
        # LastEvaluatedKey is determined before the Filter Expression is applied
        # so LastEvaluatedKey can still be present
        table.query.side_effect = [
            {"Items": [job_event_stub(Sk=str(i)) for i in range(0, 10)],
             "LastEvaluatedKey": {"Id": job["Id"], "Sk": "40"}},
            {"Items": []}
        ]
        response = handlers.list_job_events_handler({
            "pathParameters": {"job_id": "test"},
            "queryStringParameters": {"start_at": "0"},
            "multiValueQueryStringParameters": {"filter": ["EventName=QuerySucceeded"]}
        }, SimpleNamespace())
        resp_body = json.loads(response["body"])
        assert 200 == response["statusCode"]
        assert "40" == resp_body["NextStart"]

    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_returns_last_item_watermark_where_ddb_response_is_greater_than_page_size(self, table):
        job = job_stub(JobStatus="RUNNING")
        table.get_item.return_value = {"Item": job}
        # LastEvaluatedKey is determined before the Filter Expression is applied
        table.query.return_value = {"Items": [job_event_stub(Sk=str(i)) for i in range(0, 100)],
                                    "LastEvaluatedKey": {"Id": job["Id"], "Sk": "99"}}
        response = handlers.list_job_events_handler({
            "pathParameters": {"job_id": "test"},
            "queryStringParameters": {"start_at": "0"},
            "multiValueQueryStringParameters": {"filter": ["EventName=QuerySucceeded"]}
        }, SimpleNamespace())
        resp_body = json.loads(response["body"])
        assert 200 == response["statusCode"]
        assert "19" == resp_body["NextStart"]

    # Completed jobs
    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_returns_last_item_watermark_where_not_last_page_and_job_complete(self, table):
        job = job_stub(JobFinishTime=12345, JobStatus="COMPLETED")
        table.get_item.return_value = {"Item": job}
        table.query.return_value = {"Items": [job_event_stub(Sk=str(i)) for i in range(0, 20)],
                                    "LastEvaluatedKey": {"Id": job["Id"], "Sk": "19"}}
        response = handlers.list_job_events_handler({
            "queryStringParameters": {},
            "pathParameters": {"job_id": "test"},
            "multiValueQueryStringParameters": {"filter": ["EventName=QuerySucceeded"]}
        }, SimpleNamespace())
        resp_body = json.loads(response["body"])
        assert 200 == response["statusCode"]
        assert "19" == resp_body["NextStart"]

    @patch("backend.lambdas.jobs.handlers.table")
    def test_it_does_not_return_watermark_if_last_page_reached_on_complete_job(self, table):
        events = [
            "COMPLETED_CLEANUP_FAILED",
            "COMPLETED",
            "FAILED",
            "FIND_FAILED",
            "FORGET_FAILED",
            "FORGET_PARTIALLY_FAILED"
        ]
        for s in events:
            event_stub = job_event_stub()
            table.get_item.return_value = {"Item": job_stub(JobStatus=s)}
            table.query.return_value = {"Items": [event_stub]}
            response = handlers.list_job_events_handler({
                "pathParameters": {"job_id": "test"},
                "queryStringParameters": {"start_at": "0"},
                "multiValueQueryStringParameters": {"filter": ["EventName=QuerySucceeded"]}
            }, SimpleNamespace())
            resp_body = json.loads(response["body"])
            assert 200 == response["statusCode"]
            assert "NextStart" not in resp_body

    # Errors
    def test_it_rejects_invalid_filters(self):
        response = handlers.list_job_events_handler({
            "pathParameters": {"job_id": "test"},
            "queryStringParameters": {"start_at": "0"},
            "multiValueQueryStringParameters": {"filter": ["Invalid=Filter"]}
        }, SimpleNamespace())
        assert 422 == response["statusCode"]


def job_stub(job_id="test", created_at=round(datetime.datetime.utcnow().timestamp()), **kwargs):
    return {"Id": job_id, "Sk": job_id, "CreatedAt": created_at, "Type": "Job", "JobStatus": "RUNNING", **kwargs}


def job_event_stub(job_id="test", sk=None, **kwargs):
    now = round(datetime.datetime.utcnow().timestamp())
    if not sk:
        sk = "{}#{}".format(str(now), "12345")
    return {"Id": job_id, "Sk": sk, "Type": "JobEvent", "CreatedAt": now, "EventName": "QuerySucceeded", **kwargs}

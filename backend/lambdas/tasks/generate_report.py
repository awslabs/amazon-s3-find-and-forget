"""
Task for generating final report
"""
import datetime
import json
import os
import sys
from collections import defaultdict

import boto3
from decorators import with_logger

logs = boto3.client("logs")
s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.getenv("JobTableName", "S3F2_JobTable"))
summary_report_keys = [
    'JobId',
    'JobStartTime',
    'JobFinishTime',
    'JobStatus',
    'CreatedAt',
    'GSIBucket',
    'TotalQueryTimeInMillis',
    'TotalQueryScannedInBytes',
    'TotalQueryCount',
    'TotalQueryFailedCount',
    'TotalObjectUpdatedCount',
    'TotalObjectUpdateFailedCount',
]


@with_logger
def handler(event, context):
    report_data = defaultdict(list)
    # Basic Job Info
    job_id = event["JobId"]
    job_start = event["JobStartTime"]
    job_finished = event["JobFinishTime"]
    created_at = event["Input"].get("CreatedAt", round(datetime.datetime.now().timestamp()))
    gsi_bucket = event["Input"].get("GSIBucket", "0")
    report_data["JobId"] = job_id
    report_data["JobStartTime"] = job_start
    report_data["JobFinishTime"] = job_finished
    report_data["CreatedAt"] = created_at
    report_data["GSIBucket"] = gsi_bucket
    # Get All Events
    job_logs = get_job_logs(job_id)
    log_events = [json.loads(e["message"]) for e in job_logs]
    for e in log_events:
        report_data[e["EventName"]].append(e["EventData"])
    # Aggregations
    report_data.update(get_aggregated_query_stats(report_data))
    report_data.update(get_aggregated_object_stats(report_data))
    report_data["JobStatus"] = get_status(report_data)
    report_data = normalise_dates(report_data)
    # Summarise
    bucket = event["Bucket"]
    write_log(bucket, job_id, report_data)
    summary_report = {
        k: v for k, v in report_data.items() if k in summary_report_keys
    }
    write_summary(summary_report)
    return summary_report


def write_summary(report):
    table.put_item(Item=report)


def write_log(bucket, job_id, report_data):
    detailed_report_key = "reports/{}.json".format(job_id)
    s3.Object(bucket, detailed_report_key).put(Body=json.dumps(report_data))


def get_aggregated_query_stats(report):
    combined = report["QuerySucceeded"] + report["QueryFailed"]
    query_count = len(combined)
    query_time = sum([
        q["QueryStatus"].get("Statistics", {}).get("EngineExecutionTimeInMillis") for q in combined
        if q.get('QueryStatus')
    ])
    query_scanned = sum([
        q["QueryStatus"].get("Statistics", {}).get("DataScannedInBytes") for q in combined
        if q.get('QueryStatus')
    ])

    return {
        "TotalQueryTimeInMillis": query_time,
        "TotalQueryScannedInBytes": query_scanned,
        "TotalQueryCount": query_count,
        "TotalQueryFailedCount": len(report["QueryFailed"])
    }


def get_aggregated_object_stats(report):
    updated = report["ObjectUpdated"]
    failed = report["ObjectUpdateFailed"]

    return {
        "TotalObjectUpdatedCount": len(updated),
        "TotalObjectUpdateFailedCount": len(failed),
    }


def get_status(report):
    if len(report.get("Exception", [])) > 0:
        return "FAILED"
    if len(report.get("QueryFailed", [])) > 0:
        return "ABORTED"
    if len(report.get("ObjectUpdateFailed", [])) > 0:
        return "COMPLETED_WITH_ERRORS"
    return "COMPLETED"


def get_job_logs(job_id):
    log_group = os.getenv("LogGroupName", "/aws/s3f2")
    kwargs = {
        'logGroupName': log_group,
        'logStreamNamePrefix': job_id,
    }
    while True:
        resp = logs.filter_log_events(**kwargs)
        yield from resp['events']
        if not resp.get('nextToken'):
            break
        kwargs['nextToken'] = resp['nextToken']


def normalise_dates(data):
    if isinstance(data, str):
        try:
            return convert_iso8601_to_epoch(data)
        except ValueError:
            return data
    elif isinstance(data, list):
        return [normalise_dates(i) for i in data]
    elif isinstance(data, dict):
        return {k: normalise_dates(v) for k, v in data.items()}
    return data


def convert_iso8601_to_epoch(iso_time: str):
    return round(datetime.datetime.strptime(iso_time, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp())

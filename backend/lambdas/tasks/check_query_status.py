import boto3

from decorators import with_logging

client = boto3.client("athena")


@with_logging
def handler(event, context):
    execution_retries_left = event["ExecutionRetriesLeft"]
    execution_details = client.get_query_execution(QueryExecutionId=event["QueryId"])[
        "QueryExecution"
    ]
    state = execution_details["Status"]["State"]
    needs_retry = state == "FAILED" or state == "CANCELLED"
    if needs_retry:
        execution_retries_left -= 1

    result = {
        **event,
        "State": state,
        "Reason": execution_details["Status"].get("StateChangeReason", "n/a"),
        "Statistics": execution_details["Statistics"],
        "ExecutionRetriesLeft": execution_retries_left,
    }

    return result

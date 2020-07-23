import boto3

from decorators import with_logging

client = boto3.client("athena")


@with_logging
def handler(event, context):
    execution_details = client.get_query_execution(QueryExecutionId=event)[
        "QueryExecution"
    ]

    result = {
        "State": execution_details["Status"]["State"],
        "Reason": execution_details["Status"].get("StateChangeReason", "n/a"),
        "Statistics": execution_details["Statistics"],
    }

    return result

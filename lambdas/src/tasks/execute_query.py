import boto3

from decorators import with_logger

client = boto3.client("athena")


@with_logger
def handler(event, context):
    response = client.start_query_execution(
        QueryString=event["Query"],
        ResultConfiguration={
            'OutputLocation': 's3://{bucket}/{prefix}/'.format(bucket=event["Bucket"], prefix=event["Prefix"])
        },
    )

    return response["QueryExecutionId"]

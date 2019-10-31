import boto3

from decorators import with_logger

client = boto3.client("athena")


@with_logger
def handler(event, context):
    response = client.start_query_execution(
        QueryString=make_query(event["QueryData"]),
        ResultConfiguration={
            'OutputLocation': 's3://{bucket}/{prefix}/'.format(bucket=event["Bucket"], prefix=event["Prefix"])
        },
    )

    return response["QueryExecutionId"]


def make_query(query_data):
    """
    Returns a query which will look like
    SELECT $path
    FROM "db"."table"
    WHERE col1 in (matchid1, matchid2) OR col1 in (matchid1, matchid2) AND partition_key = value"
    """
    template = '''
    SELECT DISTINCT "$path"
    FROM "{db}"."{table}"
    WHERE
        ({column_filters})
    '''
    db = query_data["Database"]
    table = query_data["Table"]
    columns = query_data["Columns"]
    partition = query_data.get("Partition")

    column_filters = ""
    for i, col in enumerate(columns):
        if i > 0:
            column_filters = column_filters + " OR "
        column_filters = column_filters + "{} in ({})".format(
            col["Column"], ', '.join("'{0}'".format(u) for u in col["Users"]))
    if partition:
        template = template + " AND {key} = '{value}' ".format(key=partition["Key"], value=partition["Value"])
    return template.format(db=db, table=table, column_filters=column_filters)

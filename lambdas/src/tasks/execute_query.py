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

    :param query_data: a dict which looks like
    {
      "Database":"db",
      "Table": "table",
      "Columns": [{"Column": "col, "MatchIds": ["match"]}],
      "Partitions": [{"Key":"k", "Value":"val"}]
    }
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
    partitions = query_data.get("Partitions", [])

    column_filters = ""
    for i, col in enumerate(columns):
        if i > 0:
            column_filters = column_filters + " OR "
        column_filters = column_filters + '{} in ({})'.format(
            escape_column(col["Column"]), ', '.join("{0}".format(escape_item(m)) for m in col["MatchIds"]))
    for partition in partitions:
        template = template + ' AND {key} = {value} '.format(key=escape_column(partition["Key"]), value=escape_item(
            partition["Value"]))
    return template.format(db=db, table=table, column_filters=column_filters)


def escape_column(item):
    return '"{}"'.format(item.replace('"', '""'))


def escape_item(item):
    if item is None:
        return 'NULL'
    elif isinstance(item, (int, float)):
        return escape_number(item)
    elif isinstance(item, str):
        return escape_string(item)
    else:
        raise ValueError("Unable to process supplied value: {}".format(item))


def escape_number(item):
    return item


def escape_string(item):
    return "'{}'".format(item.replace("'", "''"))

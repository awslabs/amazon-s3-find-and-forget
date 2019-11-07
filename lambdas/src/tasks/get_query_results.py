import boto3

from decorators import with_logger
from boto_utils import paginate

client = boto3.client("athena")


@with_logger
def handler(query_id, context):
    results = paginate(client, client.get_query_results, ["ResultSet", "Rows"], **{
        "QueryExecutionId": query_id
    })
    rows = [result for result in results]
    header_row = rows.pop(0)
    path_field_index = next((index for (index, d) in enumerate(header_row["Data"]) if d["VarCharValue"] == "$path"),
                            None)

    paths = [row["Data"][path_field_index]["VarCharValue"] for row in rows]

    return paths

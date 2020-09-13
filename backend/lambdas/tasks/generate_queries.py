"""
Task for generating Athena queries from glue catalogs
"""
import os
import boto3
import json

from boto_utils import paginate, batch_sqs_msgs, deserialize_item
from decorators import with_logging

ddb = boto3.resource("dynamodb")
ddb_client = boto3.client("dynamodb")
glue_client = boto3.client("glue")
sqs = boto3.resource("sqs")

queue = sqs.Queue(os.getenv("QueryQueue"))
jobs_table = ddb.Table(os.getenv("JobTable", "S3F2_Jobs"))
data_mapper_table_name = os.getenv("DataMapperTable", "S3F2_DataMappers")
s3 = boto3.resource("s3")

ARRAYSTRUCT = "array<struct>"
ARRAYSTRUCT_PREFIX = "array<struct<"
ARRAYSTRUCT_SUFFIX = ">>"
STRUCT = "struct"
STRUCT_PREFIX = "struct<"
STRUCT_SUFFIX = ">"
SCHEMA_INVALID = "Column schema is not valid"
ALLOWED_TYPES = [
    "bigint",
    "char",
    "double",
    "float",
    "int",
    "smallint",
    "string",
    "tinyint",
    "varchar",
]


@with_logging
def handler(event, context):
    job_id = event['ExecutionName']
    bucket, deletion_items = get_deletion_queue(job_id)
    for data_mapper in get_data_mappers():
        query_executor = data_mapper["QueryExecutor"]
        if query_executor == "athena":
            queries = generate_athena_queries(data_mapper, deletion_items, bucket, job_id)
        else:
            raise NotImplementedError("Unsupported data mapper query executor: '{}'".format(query_executor))

        batch_sqs_msgs(queue, queries)


def generate_athena_queries(data_mapper, deletion_items, bucket, job_id):
    queries = []
    mapper_deletion_queue_bucket = data_mapper["DeletionQueueBucket"]
    mapper_deletion_queue_key = "{}data.csv".format(data_mapper["DeletionQueuePrefix"])

    db = data_mapper["QueryExecutorParameters"]["Database"]
    table_name = data_mapper["QueryExecutorParameters"]["Table"]
    key = "jobs/{}/query_data/{}/data.json".format(job_id, data_mapper["DataMapperId"])
    table = get_table(db, table_name)
    partition_keys = table.get("PartitionKeys", [])

    columns = [c for c in data_mapper["Columns"]]
    # Handle unpartitioned data
    msg = {
        "DeletionQueueDb": data_mapper["DeletionQueueDb"],
        "DeletionQueueTableName": data_mapper["DeletionQueueTableName"],
        "DataMapperId": data_mapper["DataMapperId"],
        "QueryExecutor": data_mapper["QueryExecutor"],
        "Format": data_mapper["Format"],
        "Database": db,
        "Table": table_name,
        "Columns": columns,
        "QueryBucket": bucket,
        "QueryKey": key,
        "AllFiles": False,  # len(deletion_items) > NUM_OF_RECORDS_IN_QUERY,
        "PartitionKeys": [],
        "DeleteOldVersions": data_mapper.get("DeleteOldVersions", True),
    }

    if data_mapper.get("RoleArn", None):
        msg["RoleArn"] = data_mapper["RoleArn"]
    if len(partition_keys) == 0:
        queries.append(msg)
    else:
        # For every partition combo of every table, create a query
        # TODO:: make sure  get_partitions() works as expected, in some cases it didn't return all partition combos
        partitions = get_partitions(db, table_name)
        for partition in partitions:
            values = partition["Values"]
            queries.append({
                **msg,
                "PartitionKeys": [
                    {"Key": partition_keys[i]["Name"], "Value": convert_to_partition_type(v, partition_keys[i]["Name"], partition_keys)}
                    for i, v in enumerate(values)
                ],
            })
    # Workout which deletion items should be included in this query
    filtered = []
    applicable_match_ids = [
        item["MatchId"] for item in deletion_items
        if data_mapper["DataMapperId"] in item.get("DataMappers", []) or len(item.get("DataMappers", [])) == 0
    ]
    if len(applicable_match_ids) > 0:
        payload = {
            "Columns": [
                {
                    "Column": c,
                    "MatchIds": [convert_to_col_type(mid, c, table) for mid in applicable_match_ids]
                } for c in columns
            ]
        }
        # save data to jobs deletion queue
        obj = s3.Object(bucket, key)
        obj.put(Body=json.dumps(payload))

        # send data to Athena deletion queue
        deletion_queue_object = s3.Object(mapper_deletion_queue_bucket, mapper_deletion_queue_key)
        dq_pl = "{}\n".format(",".join(columns)) + "\n".join(applicable_match_ids)
        deletion_queue_object.put(Body=dq_pl)

        filtered = queries
    return filtered


def get_deletion_queue(job_id):
    resp = jobs_table.get_item(Key={'Id': job_id, 'Sk': job_id})
    item = resp.get('Item')
    bucket = item.get('DeletionQueueBucket')
    key = item.get('DeletionQueueKey')
    obj = s3.Object(bucket, key)
    raw_data = obj.get()['Body'].read().decode('utf-8')
    deletion_queue_items = json.loads(raw_data)
    deletion_queue_list = deletion_queue_items["DeletionQueueItems"]
    return bucket, deletion_queue_list


def get_data_mappers():
    results = paginate(ddb_client, ddb_client.scan, "Items", TableName=data_mapper_table_name)
    for result in results:
        yield deserialize_item(result)


def get_table(db, table_name):
    return glue_client.get_table(DatabaseName=db, Name=table_name)["Table"]


def get_partitions(db, table_name):
    return paginate(glue_client, glue_client.get_partitions, ["Partitions"], DatabaseName=db, TableName=table_name)


def get_inner_children(str, prefix, suffix):
    """
    Function to get inner children from complex type string
    "struct<name:string,age:int>" => "name:string,age:int"
    """
    if not str.endswith(suffix):
        raise ValueError(SCHEMA_INVALID)
    return str[len(prefix) : -len(suffix)]


def get_nested_children(str, nested_type):
    """
    Function to get next nested child type from a children string
    starting with a complex type such as struct or array
    "struct<name:string,age:int,s:struct<n:int>>,b:string" =>
    "struct<name:string,age:int,s:struct<n:int>>"
    """
    is_struct = nested_type == STRUCT
    prefix = STRUCT_PREFIX if is_struct else ARRAYSTRUCT_PREFIX
    suffix = STRUCT_SUFFIX if is_struct else ARRAYSTRUCT_SUFFIX
    n_opened_tags = len(suffix)
    end_index = -1
    to_parse = str[len(prefix) :]
    for i in range(len(to_parse)):
        char = to_parse[i : (i + 1)]
        if char == "<":
            n_opened_tags += 1
        if char == ">":
            n_opened_tags -= 1
        if n_opened_tags == 0:
            end_index = i
            break
    if end_index < 0:
        raise ValueError(SCHEMA_INVALID)
    return str[0 : (end_index + len(prefix) + 1)]


def get_nested_type(str):
    """
    Function to get next nested child type from a children string
    starting with a non complex type
    "string,a:int" => "string"
    """
    upper_index = str.find(",")
    return str[0:upper_index] if upper_index >= 0 else str


def set_no_identifier_to_node_and_its_children(node):
    """
    Function to set canBeIdentifier=false to item and its children
    Example:
    {
        name: "arr",
        type: "array<struct>",
        canBeIdentifier: false,
        children: [
            { name: "field", type: "int", canBeIdentifier: true },
            { name: "n", type: "string", canBeIdentifier: true }
        ]
    } => {
        name: "arr",
        type: "array<struct>",
        canBeIdentifier: false,
        children: [
            { name: "field", type: "int", canBeIdentifier: false },
            { name: "n", type: "string", canBeIdentifier: false }
        ]
    }
    """
    node["CanBeIdentifier"] = False
    for child in node.get("Children", []):
        set_no_identifier_to_node_and_its_children(child)


def column_mapper(col):
    """
    Function to map Columns from AWS Glue schema to tree
    Example 1:
    { Name: "Name", Type: "int" } =>
    { name: "Name", type: "int", canBeIdentifier: true }
    Example 2:
    { Name: "complex", Type: "struct<a:string,b:struct<c:int>>"} =>
    { name: "complex", type: "struct", children: [
        { name: "a", type: "string", canBeIdentifier: false},
        { name: "b", type: "struct", children: [
        { name: "c", type: "int", canBeIdentifier: false}
        ], canBeIdentifier: false}
    ], canBeIdentifier: false}
    """
    prefix = suffix = None
    result_type = col["Type"]
    has_children = False

    if result_type.startswith(ARRAYSTRUCT_PREFIX):
        result_type = ARRAYSTRUCT
        prefix = ARRAYSTRUCT_PREFIX
        suffix = ARRAYSTRUCT_SUFFIX
        has_children = True
    elif result_type.startswith(STRUCT_PREFIX):
        result_type = STRUCT
        prefix = STRUCT_PREFIX
        suffix = STRUCT_SUFFIX
        has_children = True

    result = {
        "Name": col["Name"],
        "Type": result_type,
        "CanBeIdentifier": col["CanBeIdentifier"]
        if "CanBeIdentifier" in col
        else result_type in ALLOWED_TYPES,
    }

    if has_children:
        result["Children"] = []
        children_to_parse = get_inner_children(col["Type"], prefix, suffix)

        while len(children_to_parse) > 0:
            sep = ":"
            name = children_to_parse[0 : children_to_parse.index(sep)]
            rest = children_to_parse[len(name) + len(sep) :]
            nested_type = "other"
            if rest.startswith(STRUCT_PREFIX):
                nested_type = STRUCT
            elif rest.startswith(ARRAYSTRUCT_PREFIX):
                nested_type = ARRAYSTRUCT

            c_type = (
                get_nested_type(rest)
                if nested_type == "other"
                else get_nested_children(rest, nested_type)
            )
            result["Children"].append(
                column_mapper(
                    {
                        "Name": name,
                        "Type": c_type,
                        "CanBeIdentifier": c_type in ALLOWED_TYPES,
                    }
                )
            )
            children_to_parse = children_to_parse[len(name) + len(sep) + len(c_type) :]
            if children_to_parse.startswith(","):
                children_to_parse = children_to_parse[1:]

        if result_type != STRUCT:
            set_no_identifier_to_node_and_its_children(result)

    return result


def get_column_info(col, table):
    table_columns = table["StorageDescriptor"]["Columns"]
    col_array = col.split(".")
    serialized_cols = list(map(column_mapper, table_columns))
    found = None
    for col_segment in col_array:
        found = next((x for x in serialized_cols if x["Name"] == col_segment), None)
        if not found:
            return None, False
        serialized_cols = found["Children"] if "Children" in found else []
    return found["Type"], found["CanBeIdentifier"]


def convert_to_partition_type(val, col, col_descriptor):
    # this way partitions can be of any type and not only string
    column = next((i for i in col_descriptor if i["Name"] == col), None)
    if not column:
        raise ValueError("Column {} not found".format(col))

    col_type = column["Type"]

    if col_type == "string" or col_type == "varchar":
        return str(val)
    if col_type == "int" or col_type == "bigint":
        return int(val)

    raise ValueError("Column {} is type {} which is not a supported column type for querying")


def convert_to_col_type(val, col, table):
    col_type, can_be_identifier = get_column_info(col, table)
    if not col_type:
        raise ValueError("Column {} not found".format(col))
    elif not can_be_identifier:
        raise ValueError(
            "Column {} is not a supported column type for querying".format(col)
        )

    if col_type in ("bigint", "int", "smallint", "tinyint"):
        return int(val)
    if col_type in ("double", "float"):
        return float(val)

    return str(val)

"""
Task for generating Athena queries from glue catalogs
"""
import os
import boto3

from boto_utils import paginate, batch_sqs_msgs, deserialize_item
from decorators import with_logging

ddb = boto3.resource("dynamodb")
ddb_client = boto3.client("dynamodb")
glue_client = boto3.client("glue")
sqs = boto3.resource("sqs")

queue = sqs.Queue(os.getenv("QueryQueue"))
jobs_table = ddb.Table(os.getenv("JobTable", "S3F2_Jobs"))
data_mapper_table_name = os.getenv("DataMapperTable", "S3F2_DataMappers")

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
    deletion_items = get_deletion_queue(event["ExecutionName"])
    for data_mapper in get_data_mappers():
        query_executor = data_mapper["QueryExecutor"]
        if query_executor == "athena":
            queries = generate_athena_queries(data_mapper, deletion_items)
        else:
            raise NotImplementedError(
                "Unsupported data mapper query executor: '{}'".format(query_executor)
            )

        batch_sqs_msgs(queue, queries)


def generate_athena_queries(data_mapper, deletion_items):
    queries = []
    db = data_mapper["QueryExecutorParameters"]["Database"]
    table_name = data_mapper["QueryExecutorParameters"]["Table"]
    table = get_table(db, table_name)
    partition_keys = [p["Name"] for p in table.get("PartitionKeys", [])]
    columns = [c for c in data_mapper["Columns"]]
    # Handle unpartitioned data
    msg = {
        "DataMapperId": data_mapper["DataMapperId"],
        "QueryExecutor": data_mapper["QueryExecutor"],
        "Format": data_mapper["Format"],
        "Database": db,
        "Table": table_name,
        "Columns": columns,
        "PartitionKeys": [],
        "DeleteOldVersions": data_mapper.get("DeleteOldVersions", True),
    }
    if data_mapper.get("RoleArn", None):
        msg["RoleArn"] = data_mapper["RoleArn"]
    if len(partition_keys) == 0:
        queries.append(msg)
    else:
        # For every partition combo of every table, create a query
        partitions = get_partitions(db, table_name)
        for partition in partitions:
            values = partition["Values"]
            queries.append(
                {
                    **msg,
                    "PartitionKeys": [
                        {
                            "Key": partition_keys[i],
                            "Value": cast_to_type(v, partition_keys[i], table, True),
                        }
                        for i, v in enumerate(values)
                    ],
                }
            )
    # Workout which deletion items should be included in this query
    filtered = []
    for i, query in enumerate(queries):
        applicable_match_ids = [
            item["MatchId"]
            for item in deletion_items
            if query["DataMapperId"] in item.get("DataMappers", [])
            or len(item.get("DataMappers", [])) == 0
        ]

        # Remove the query if there are no relevant matches
        if len(applicable_match_ids) == 0:
            continue
        else:
            simple_applicable_match_ids = [
                item for item in applicable_match_ids if not isinstance(item, list)
            ]
            query["Columns"] = (
                [
                    {
                        "Column": c,
                        "MatchIds": [
                            cast_to_type(mid, c, table)
                            for mid in simple_applicable_match_ids
                        ],
                    }
                    for c in queries[i]["Columns"]
                ]
                if len(simple_applicable_match_ids) > 0
                else []
            )
            composite_applicable_match_ids = [
                item for item in applicable_match_ids if isinstance(item, list)
            ]
            query["CompositeColumns"] = []
            for mid in composite_applicable_match_ids:
                sorted_mid = sorted(mid, key=lambda x: x["Column"])
                cols = list(map(lambda x: x["Column"], sorted_mid))
                composite_column = next(
                    iter(
                        [x for x in query["CompositeColumns"] if x["Columns"] == cols]
                    ),
                    None,
                )
                composite_match = list(
                    map(
                        lambda x: cast_to_type(x["Value"], x["Column"], table),
                        sorted_mid,
                    )
                )
                if composite_column:
                    composite_column["MatchIds"].append(composite_match)
                else:
                    query["CompositeColumns"].append(
                        {"Columns": cols, "MatchIds": [composite_match]}
                    )
            filtered.append(query)
    return filtered


def get_deletion_queue(job_id):
    resp = jobs_table.get_item(Key={"Id": job_id, "Sk": job_id})
    return resp.get("Item").get("DeletionQueueItems")


def get_data_mappers():
    results = paginate(
        ddb_client, ddb_client.scan, "Items", TableName=data_mapper_table_name
    )
    for result in results:
        yield deserialize_item(result)


def get_table(db, table_name):
    return glue_client.get_table(DatabaseName=db, Name=table_name)["Table"]


def get_partitions(db, table_name):
    return paginate(
        glue_client,
        glue_client.get_partitions,
        ["Partitions"],
        DatabaseName=db,
        TableName=table_name,
    )


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


def get_column_info(col, table, is_partition):
    table_columns = (
        table["PartitionKeys"]
        if is_partition
        else table["StorageDescriptor"]["Columns"]
    )
    col_array = col.split(".")
    serialized_cols = list(map(column_mapper, table_columns))
    found = None
    for col_segment in col_array:
        found = next((x for x in serialized_cols if x["Name"] == col_segment), None)
        if not found:
            return None, False
        serialized_cols = found["Children"] if "Children" in found else []
    return found["Type"], found["CanBeIdentifier"]


def cast_to_type(val, col, table, is_partition=False):
    col_type, can_be_identifier = get_column_info(col, table, is_partition)
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

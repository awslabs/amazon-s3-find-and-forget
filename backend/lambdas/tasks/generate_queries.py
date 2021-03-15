"""
Task for generating Athena queries from glue catalog aka Query Planning
"""
import json
import os
import boto3

from operator import itemgetter
from boto_utils import paginate, batch_sqs_msgs, deserialize_item, DecimalEncoder
from decorators import with_logging

ddb = boto3.resource("dynamodb")
ddb_client = boto3.client("dynamodb")
glue_client = boto3.client("glue")
s3 = boto3.resource("s3")
sqs = boto3.resource("sqs")

queue = sqs.Queue(os.getenv("QueryQueue"))
jobs_table = ddb.Table(os.getenv("JobTable", "S3F2_Jobs"))
data_mapper_table_name = os.getenv("DataMapperTable", "S3F2_DataMappers")
deletion_queue_table_name = os.getenv("DeletionQueueTable", "S3F2_DeletionQueue")
manifests_bucket_name = os.getenv("ManifestsBucket", "S3F2-manifests-bucket")
glue_db = os.getenv("GlueDatabase", "s3f2_manifests_database")
glue_table = os.getenv("JobManifestsGlueTable", "s3f2_manifests_table")

COMPOSITE_JOIN_TOKEN = "_S3F2COMP_"
MANIFEST_KEY = "manifests/{job_id}/{data_mapper_id}/manifest.json"

COMPOSITE_JOIN_TOKEN = "_S3F2COMP_"

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
    job_id = event["ExecutionName"]
    deletion_items = get_deletion_queue()
    manifests_partitions = []
    data_mappers = get_data_mappers()
    total_queries = 0
    for data_mapper in data_mappers:
        query_executor = data_mapper["QueryExecutor"]
        if query_executor == "athena":
            queries = generate_athena_queries(data_mapper, deletion_items, job_id)
            if len(queries) > 0:
                manifests_partitions.append([job_id, data_mapper["DataMapperId"]])
        else:
            raise NotImplementedError(
                "Unsupported data mapper query executor: '{}'".format(query_executor)
            )

        batch_sqs_msgs(queue, queries)
        total_queries += len(queries)
    write_partitions(manifests_partitions)
    return {
        "GeneratedQueries": total_queries,
        "DeletionQueueSize": len(deletion_items),
        "Manifests": [
            "s3://{}/{}".format(
                manifests_bucket_name,
                MANIFEST_KEY.format(
                    job_id=partition_tuple[0], data_mapper_id=partition_tuple[1]
                ),
            )
            for partition_tuple in manifests_partitions
        ],
    }


def build_manifest_row(columns, match_id, item_id, item_createdat):
    """
    Function for building each row of the manifest that will be written to S3.

    * What are 'queryablematchid' and 'queryablecolumns'?
    A convenience stringified value of match_id and its column when the match
    is simple, or a stringified joint value when composite (for instance,
    "John_S3F2COMP_Doe" and "first_name_S3F2COMP_last_name"). The purpose of
    these fields is optimise query execution by doing the SQL JOINs over strings only.
    
    * What are MatchId and Columns?
    Original values to be used by the ECS task instead.
    Note that the MatchId is declared as array<string> in the Glue Table as it's
    not possible to declare it as array of generic types and the design is for
    using a single table schema for each match/column tuple, despite
    the current column type.
    This means that using the "MatchId" field in Athena will always coherce its values
    to strings, for instance [1234] => ["1234"]. That's ok because when working with
    the manifest, the Fargate task will read and parse the JSON directly and therefore
    will use its original type (for instance, int over strings to do the comparison).
    """
    is_composite = len(columns) > 1
    iterable_match = match_id if is_composite else [match_id]
    queryable = COMPOSITE_JOIN_TOKEN.join(str(x) for x in iterable_match)
    queryable_cols = COMPOSITE_JOIN_TOKEN.join(str(x) for x in columns)
    return (
        json.dumps(
            {
                "Columns": columns,
                "MatchId": iterable_match,
                "DeletionQueueItemId": item_id,
                "CreatedAt": item_createdat,
                "QueryableColumns": queryable_cols,
                "QueryableMatchId": queryable,
            },
            cls=DecimalEncoder,
        )
        + "\n"
    )


def generate_athena_queries(data_mapper, deletion_items, job_id):
    """
    For each Data Mapper, it generates a list of parameters needed for each
    query execution. The matches for the given column are saved in an external
    S3 object (aka manifest) to allow its size to grow into the thousands without
    incurring in DDB Document size limit, SQS message size limit, or Athena query
    size limit. The manifest S3 Path is finally referenced as part of the SQS message.
    """
    manifest_key = MANIFEST_KEY.format(
        job_id=job_id, data_mapper_id=data_mapper["DataMapperId"]
    )
    db = data_mapper["QueryExecutorParameters"]["Database"]
    table_name = data_mapper["QueryExecutorParameters"]["Table"]
    table = get_table(db, table_name)
    partition_keys = [p["Name"] for p in table.get("PartitionKeys", [])]
    columns = [c for c in data_mapper["Columns"]]
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

    # Workout which deletion items should be included in this query
    applicable_match_ids = [
        item
        for item in deletion_items
        if msg["DataMapperId"] in item.get("DataMappers", [])
        or len(item.get("DataMappers", [])) == 0
    ]
    if len(applicable_match_ids) == 0:
        return []

    # Compile a list of MatchIds grouped by Column
    columns_with_matches = {}
    manifest = ""
    for item in applicable_match_ids:
        mid, item_id, item_createdat = itemgetter(
            "MatchId", "DeletionQueueItemId", "CreatedAt"
        )(item)
        is_simple = not isinstance(mid, list)
        if is_simple:
            for column in msg["Columns"]:
                casted = cast_to_type(mid, column, table)
                if column not in columns_with_matches:
                    columns_with_matches[column] = {
                        "Column": column,
                        "Type": "Simple",
                    }
                manifest += build_manifest_row(
                    [column], casted, item_id, item_createdat
                )
        else:
            sorted_mid = sorted(mid, key=lambda x: x["Column"])
            query_columns = list(map(lambda x: x["Column"], sorted_mid))
            column_key = COMPOSITE_JOIN_TOKEN.join(query_columns)
            composite_match = list(
                map(lambda x: cast_to_type(x["Value"], x["Column"], table), sorted_mid)
            )
            if column_key not in columns_with_matches:
                columns_with_matches[column_key] = {
                    "Columns": query_columns,
                    "Type": "Composite",
                }
            manifest += build_manifest_row(
                query_columns, composite_match, item_id, item_createdat
            )
    s3.Bucket(manifests_bucket_name).put_object(Body=manifest, Key=manifest_key)
    msg["Columns"] = list(columns_with_matches.values())
    msg["Manifest"] = "s3://{}/{}".format(manifests_bucket_name, manifest_key)

    if len(partition_keys) == 0:
        return [msg]

    # For every partition combo of every table, create a query
    return list(
        map(
            lambda x: {
                **msg,
                "PartitionKeys": [
                    {
                        "Key": partition_keys[i],
                        "Value": cast_to_type(v, partition_keys[i], table, True),
                    }
                    for i, v in enumerate(x["Values"])
                ],
            },
            get_partitions(db, table_name),
        )
    )


def get_deletion_queue():
    results = paginate(
        ddb_client, ddb_client.scan, "Items", TableName=deletion_queue_table_name
    )
    return [deserialize_item(result) for result in results]


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


def write_partitions(partitions):
    """
    In order for the manifests to be used by Athena in a JOIN, we make them
    available as partitions with Job and DataMapperId tuple.
    """
    max_create_batch_size = 100
    for i in range(0, len(partitions), max_create_batch_size):
        glue_client.batch_create_partition(
            DatabaseName=glue_db,
            TableName=glue_table,
            PartitionInputList=[
                {
                    "Values": partition_tuple,
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "columns", "Type": "array<string>"},
                            {"Name": "matchid", "Type": "array<string>"},
                            {"Name": "deletionqueueitemid", "Type": "string"},
                            {"Name": "createdat", "Type": "int"},
                            {"Name": "queryablecolumns", "Type": "string"},
                            {"Name": "queryablematchid", "Type": "string"},
                        ],
                        "Location": "s3://{}/manifests/{}/{}/".format(
                            manifests_bucket_name,
                            partition_tuple[0],
                            partition_tuple[1],
                        ),
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                        "Compressed": False,
                        "SerdeInfo": {
                            "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                        },
                        "StoredAsSubDirectories": False,
                    },
                }
                for partition_tuple in partitions[i : i + max_create_batch_size]
            ],
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

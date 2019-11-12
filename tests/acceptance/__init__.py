import logging
import tempfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from cfn_flip import load
from dotenv import load_dotenv

logger = logging.getLogger()


def load_env():
    load_dotenv(".env")


def load_template(template_name):
    project_root = Path(__file__).parent.parent.absolute()
    with open(project_root.joinpath("templates", template_name)) as f:
        return load(f.read())[0]


def get_resources_from_template(template, resource_type=None):
    resources = template["Resources"]
    if not resource_type:
        return resources

    return {k: v for k, v in resources.items() if v["Type"] == resource_type}


def get_schema_from_template(ddb_template, logical_identifier):
    resource = ddb_template["Resources"].get(logical_identifier)
    if not resource:
        raise KeyError("Unable to find resource with identifier %s", logical_identifier)

    return {
        k["KeyType"]: k["AttributeName"] for k in resource["Properties"]["KeySchema"]
    }


def generate_parquet_file(items, columns):
    df = pd.DataFrame(items, columns=columns)
    table = pa.Table.from_pandas(df)
    tmp = tempfile.TemporaryFile()
    pq.write_table(table, tmp)
    return tmp


def query_parquet_file(f, column, val):
    table = pq.read_table(f)
    return [i for i in table.column(column) if i == val]


def empty_table(table, key):
    items = table.scan()["Items"]
    with table.batch_writer() as batch:
        for item in items:
            batch.delete_item(
                Key={
                    key: item[key],
                }
            )
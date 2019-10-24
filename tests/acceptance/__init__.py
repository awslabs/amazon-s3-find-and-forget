import logging
from os import path

from cfn_flip import load
from dotenv import load_dotenv

logger = logging.getLogger()


def load_env():
    load_dotenv(".env")


def load_template(template_name):
    project_root = path.dirname(path.dirname(path.dirname(__file__)))
    with open(path.join(project_root, "templates", template_name)) as f:
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

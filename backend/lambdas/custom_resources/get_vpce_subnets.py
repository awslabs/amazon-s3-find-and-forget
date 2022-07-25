#############################################################
# This Custom Resource is required since VPC Endpoint names #
# and subnets are not consistant in the China region        #
#############################################################

import boto3
from crhelper import CfnResource
from decorators import with_logging

helper = CfnResource(json_logging=False, log_level="DEBUG", boto_level="CRITICAL")

ec2_client = boto3.client("ec2")


@with_logging
@helper.create
@helper.update
def create(event, context):
    props = event.get("ResourceProperties", None)
    service_name = props.get("ServiceName")
    subnet_ids = props.get("SubnetIds")
    vpc_endpoint_type = props.get("VpcEndpointType")
    describe_subnets = ec2_client.describe_subnets(SubnetIds=subnet_ids)
    subnet_dict = {
        s["AvailabilityZone"]: s["SubnetId"] for s in describe_subnets["Subnets"]
    }
    endpoint_service = ec2_client.describe_vpc_endpoint_services(
        Filters=[
            {"Name": "service-name", "Values": [f"cn.{service_name}", service_name]},
            {"Name": "service-type", "Values": [vpc_endpoint_type]},
        ]
    )
    service_details = endpoint_service["ServiceDetails"][0]
    helper.Data["ServiceName"] = service_details["ServiceName"]
    return ",".join([subnet_dict[s] for s in service_details["AvailabilityZones"]])


@with_logging
@helper.delete
def delete(event, context):
    return None


def handler(event, context):
    helper(event, context)

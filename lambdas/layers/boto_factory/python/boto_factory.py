"""
Factory for boto3
"""
import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Supported mock services
service_map = {
    "dynamodb": "{proto}://{host}:8000",
}


def _get_local_service_endpoint(service, host=None, proto="http"):
    try:
        template = service_map[service]
        if not host:
            host = service
        return template.format(host=host, proto=proto)

    except KeyError as e:
        logger.error("%s not supported locally", service)
        raise e


def get_client(service, **kwargs):
    is_sam_local = os.getenv("AWS_SAM_LOCAL", False)
    if is_sam_local and service_map.get(service, None):
        logger.info("Accessing supported local service '%s' client from SAM local", service)
        return boto3.client(service,
                            endpoint_url=_get_local_service_endpoint(service),
                            aws_access_key_id="test", aws_secret_access_key="test", **kwargs)
    return boto3.resource(service, **kwargs)


def get_resource(service, **kwargs):
    is_sam_local = os.getenv("AWS_SAM_LOCAL", False)
    if is_sam_local and service_map.get(service, None):
        logger.info("Accessing supported local service '%s' resource from SAM local", service)
        return boto3.resource(service,
                              endpoint_url=_get_local_service_endpoint(service),
                              aws_access_key_id="test", aws_secret_access_key="test", **kwargs)
    return boto3.resource(service, **kwargs)

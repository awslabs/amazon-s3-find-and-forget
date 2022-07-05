import pytest
from mock import patch, MagicMock

from backend.lambdas.custom_resources.get_vpce_subnets import (
    create,
    delete,
    handler,
)

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.custom_resources.get_vpce_subnets.ec2_client")
def test_it_returns_valid_subnets(mock_client):
    event = {
        "ResourceProperties": {
            "ServiceName": "com.amazonaws.eu-west-2.monitoring",
            "SubnetIds": ["subnet-0123456789abcdef0", "subnet-0123456789abcdef1"],
            "VpcEndpointType": "Interface",
        }
    }

    mock_client.describe_subnets.return_value = {
        "Subnets": [
            {
                "AvailabilityZone": "eu-west-2a",
                "SubnetId": "subnet-0123456789abcdef0",
            },
            {
                "AvailabilityZone": "eu-west-2b",
                "SubnetId": "subnet-0123456789abcdef1",
            },
        ]
    }

    mock_client.describe_vpc_endpoint_services.return_value = {
        "ServiceDetails": [
            {
                "ServiceName": "com.amazonaws.eu-west-2.monitoring",
                "AvailabilityZones": [
                    "eu-west-2a",
                    "eu-west-2b",
                ],
            }
        ]
    }

    resp = create(event, MagicMock())

    mock_client.describe_subnets.assert_called_with(
        SubnetIds=["subnet-0123456789abcdef0", "subnet-0123456789abcdef1"]
    )
    mock_client.describe_vpc_endpoint_services.assert_called_with(
        Filters=[
            {
                "Name": "service-name",
                "Values": [
                    "cn.com.amazonaws.u-west-2.monitoring",
                    "com.amazonaws.eu-west-2.monitoring",
                ],
            },
            {"Name": "service-type", "Values": ["Interface"]},
        ]
    )

    assert resp == "subnet-0123456789abcdef0,subnet-0123456789abcdef1"


@patch("backend.lambdas.custom_resources.get_vpce_subnets.ec2_client")
def test_it_raises_exception(mock_client):
    event = {
        "ResourceProperties": {
            "ServiceName": "com.amazonaws.eu-west-2.dummy",
            "SubnetIds": [],
            "VpcEndpointType": "Interface",
        }
    }

    mock_client.describe_subnets.return_value = {
        "Subnets": [
            {
                "AvailabilityZone": "eu-west-2a",
                "SubnetId": "subnet-0123456789abcdef0",
            },
            {
                "AvailabilityZone": "eu-west-2b",
                "SubnetId": "subnet-0123456789abcdef1",
            },
        ]
    }

    mock_client.describe_vpc_endpoint_services.return_value = {"ServiceDetails": []}

    with pytest.raises(Exception) as e_info:
        create(event, MagicMock())

    mock_client.describe_subnets.assert_called_with(SubnetIds=[])
    mock_client.describe_vpc_endpoint_services.assert_called_with(
        Filters=[
            {
                "Name": "service-name",
                "Values": [
                    "cn.com.amazonaws.u-west-2.dummy",
                    "com.amazonaws.eu-west-2.dummy",
                ],
            },
            {"Name": "service-type", "Values": ["Interface"]},
        ]
    )

    assert e_info.typename == "IndexError"


@patch("backend.lambdas.custom_resources.get_vpce_subnets.ec2_client")
def test_it_does_nothing_on_delete(mock_client):
    resp = delete({}, MagicMock())

    mock_client.assert_not_called()
    assert not resp


@patch("backend.lambdas.custom_resources.get_vpce_subnets.helper")
def test_it_delegates_to_cr_helper(cr_helper):
    handler(1, 2)
    cr_helper.assert_called_with(1, 2)

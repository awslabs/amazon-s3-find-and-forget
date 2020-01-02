from types import SimpleNamespace

import json
import pytest
from mock import patch, MagicMock

from backend.lambdas.custom_resources.create_settings import create, delete, handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.custom_resources.create_settings.s3_client")
def test_it_saves_file_with_public_acl_when_cloudfront_false(mock_client):
    event = {
        'ResourceProperties': {
            'ApiUrl': 'https://apiurl/',
            'AthenaExecutionRole': 'athena-role',
            'CognitoIdentityPoolId': 'cognito-idp',
            'CognitoUserPoolClientId': 'cognito-upc',
            'CognitoUserPoolId': 'cognito-up',
            'CreateCloudFrontDistribution': 'false',
            'DeleteTaskRole': 'delete-role',
            'Region': 'eu-west-1',
            'Version': '1.0',
            'WebUIBucket': 'webuibucket'
        }
    }

    resp = create(event, MagicMock())

    mock_client.put_object.assert_called_with(
        ACL="public-read",
        Bucket="webuibucket",
        Key="settings.js",
        Body="window.s3f2Settings={}".format(json.dumps({
            "apiUrl": "https://apiurl/",
            "athenaExecutionRole": "athena-role",
            "cognitoIdentityPool": "cognito-idp",
            "cognitoUserPoolId": "cognito-up",
            "cognitoUserPoolClientId": "cognito-upc",
            "deleteTaskRole": "delete-role",
            "region": "eu-west-1",
            "version": "1.0"
        })))

    assert "arn:aws:s3:::webuibucket/settings.js" == resp


@patch("backend.lambdas.custom_resources.create_settings.s3_client")
def test_it_saves_file_with_private_acl_when_cloudfront_true(mock_client):
    event = {
        'ResourceProperties': {
            'ApiUrl': 'https://apiurl/',
            'AthenaExecutionRole': 'athena-role',
            'CognitoIdentityPoolId': 'cognito-idp',
            'CognitoUserPoolClientId': 'cognito-upc',
            'CognitoUserPoolId': 'cognito-up',
            'CreateCloudFrontDistribution': 'true',
            'DeleteTaskRole': 'delete-role',
            'Region': 'eu-west-1',
            'Version': '1.0',
            'WebUIBucket': 'webuibucket'
        }
    }

    resp = create(event, MagicMock())

    mock_client.put_object.assert_called_with(
        ACL="private",
        Bucket="webuibucket",
        Key="settings.js",
        Body="window.s3f2Settings={}".format(json.dumps({
            "apiUrl": "https://apiurl/",
            "athenaExecutionRole": "athena-role",
            "cognitoIdentityPool": "cognito-idp",
            "cognitoUserPoolId": "cognito-up",
            "cognitoUserPoolClientId": "cognito-upc",
            "deleteTaskRole": "delete-role",
            "region": "eu-west-1",
            "version": "1.0"
        })))

    assert "arn:aws:s3:::webuibucket/settings.js" == resp


@patch("backend.lambdas.custom_resources.create_settings.s3_client")
def test_it_does_nothing_on_delete(mock_client):
    event = {
        'ResourceProperties': {
            'ApiUrl': 'https://apiurl/',
            'AthenaExecutionRole': 'athena-role',
            'CognitoIdentityPoolId': 'cognito-idp',
            'CognitoUserPoolClientId': 'cognito-upc',
            'CognitoUserPoolId': 'cognito-up',
            'CreateCloudFrontDistribution': 'true',
            'DeleteTaskRole': 'delete-role',
            'Region': 'eu-west-1',
            'Version': '1.0',
            'WebUIBucket': 'webuibucket'
        }
    }
    resp = delete(event, MagicMock())
    mock_client.assert_not_called()
    assert not resp


@patch("backend.lambdas.custom_resources.create_settings.helper")
def test_it_delegates_to_cr_helper(cr_helper):
    handler(1, 2)
    cr_helper.assert_called_with(1, 2)

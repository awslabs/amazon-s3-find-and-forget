from types import SimpleNamespace

import json
import pytest
from mock import patch, MagicMock

from backend.lambdas.custom_resources.create_settings import create, delete, handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.custom_resources.create_settings.bucket", "webuibucket")
@patch("backend.lambdas.custom_resources.create_settings.s3_client")
@patch("os.getenv")
def test_it_saves_file_with_public_acl_when_cloudfront_false(getenv_mock, mock_client):

    with_cloudfront = "false"
    getenv_mock.side_effect = [with_cloudfront, "https://apiurl/",
                               "cognito-idp", "cognito-up", "cognito-upc", "eu-west-1"]

    resp = create({}, MagicMock())

    mock_client.put_object.assert_called_with(
        ACL="public-read",
        Bucket="webuibucket",
        Key="settings.js",
        Body="window.s3f2Settings={}".format(json.dumps({
            "apiUrl": "https://apiurl/",
            "cognitoIdentityPool": "cognito-idp",
            "cognitoUserPoolId": "cognito-up",
            "cognitoUserPoolClientId": "cognito-upc",
            "region": "eu-west-1"
        })))

    assert "arn:aws:s3:::webuibucket/settings.js" == resp


@patch("backend.lambdas.custom_resources.create_settings.bucket", "webuibucket")
@patch("backend.lambdas.custom_resources.create_settings.s3_client")
@patch("os.getenv")
def test_it_saves_file_with_private_acl_when_cloudfront_true(getenv_mock, mock_client):

    with_cloudfront = "true"
    getenv_mock.side_effect = [with_cloudfront, "https://apiurl/",
                               "cognito-idp", "cognito-up", "cognito-upc", "eu-west-1"]

    resp = create({}, MagicMock())

    mock_client.put_object.assert_called_with(
        ACL="private",
        Bucket="webuibucket",
        Key="settings.js",
        Body="window.s3f2Settings={}".format(json.dumps({
            "apiUrl": "https://apiurl/",
            "cognitoIdentityPool": "cognito-idp",
            "cognitoUserPoolId": "cognito-up",
            "cognitoUserPoolClientId": "cognito-upc",
            "region": "eu-west-1"
        })))

    assert "arn:aws:s3:::webuibucket/settings.js" == resp


@patch("backend.lambdas.custom_resources.create_settings.bucket", "webuibucket")
@patch("backend.lambdas.custom_resources.create_settings.s3_client")
@patch("os.getenv")
def test_it_deletes_file(getenv_mock, mock_client):

    resp = delete({}, MagicMock())

    mock_client.delete_object.assert_called_with(
        Bucket="webuibucket",
        Key="settings.js")

    assert None == resp


@patch("backend.lambdas.custom_resources.create_settings.helper")
def test_it_delegates_to_cr_helper(cr_helper):
    handler(1, 2)
    cr_helper.assert_called_with(1, 2)

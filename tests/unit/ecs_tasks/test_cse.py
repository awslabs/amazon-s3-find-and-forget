from botocore.exceptions import ClientError
from mock import patch, MagicMock, call, ANY
import pytest

import base64
import json
from io import BytesIO
import os

from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.padding import PKCS7
from cryptography.hazmat.primitives.ciphers.modes import ECB, GCM

from backend.ecs_tasks.delete_files.cse import is_kms_cse_encrypted, encrypt, decrypt

pytestmark = [pytest.mark.unit, pytest.mark.ecs_tasks]


class KmsMock:
    """
    This is not what the real KMS does.
    Only used for unit testing.
    """

    def __init__(self, key_id):
        plaintext = os.urandom(32)
        iv = os.urandom(16)
        encryptor = Cipher(AES(base64.b64decode(key_id)), GCM(iv)).encryptor()
        aad = BytesIO()
        aad.write("kms_cmk_id".encode("utf-8"))
        aad.write(key_id.encode("utf-8"))
        encryptor.authenticate_additional_data(aad.getvalue())
        ciphertext = encryptor.update(plaintext) + encryptor.finalize()
        self.key_id = key_id
        self.plaintext = plaintext
        self.ciphertext = ciphertext

    def generate_data_key(self):
        return {
            "Plaintext": self.plaintext,
            "CiphertextBlob": self.ciphertext,
            "KeyId": self.key_id,
        }

    def decrypt(self):
        return {"Plaintext": self.plaintext}


def test_it_recognises_supported_kms_cse_object():
    valid_cbc = {
        "x-amz-key-v2": "key",
        "x-amz-wrap-alg": "kms",
        "x-amz-cek-alg": "AES/CBC/PKCS5Padding",
    }
    valid_gcm = {
        "x-amz-key-v2": "key",
        "x-amz-wrap-alg": "kms",
        "x-amz-cek-alg": "AES/GCM/NoPadding",
    }
    not_encrypted = {}
    assert is_kms_cse_encrypted(valid_cbc)
    assert is_kms_cse_encrypted(valid_gcm)
    assert not is_kms_cse_encrypted(not_encrypted)


def test_it_throws_exception_for_encryption_sdk_v1():
    old_sdk = {
        "x-amz-key": "key",
        "x-amz-wrap-alg": "kms",
        "x-amz-cek-alg": "AES/CBC/PKCS5Padding",
    }
    with pytest.raises(ValueError) as e:
        is_kms_cse_encrypted(old_sdk)
    assert e.value.args[0] == "Unsupported Encryption SDK version"


def test_it_throws_exception_for_unsupported_encryption_algorithm():
    invalid_algorithm = {
        "x-amz-key-v2": "key",
        "x-amz-wrap-alg": "kms",
        "x-amz-cek-alg": "AES/FOO/Bar",
    }
    with pytest.raises(ValueError) as e:
        is_kms_cse_encrypted(invalid_algorithm)
    assert e.value.args[0] == "Unsupported Encryption algorithm"


def test_it_throws_exception_for_unsupported_encryption_strategy():
    not_kms = {"x-amz-key-v2": "key", "x-amz-cek-alg": "AES/CBC/PKCS5Padding"}
    with pytest.raises(ValueError) as e:
        is_kms_cse_encrypted(not_kms)
    assert e.value.args[0] == "Unsupported Encryption strategy"


def test_it_encrypts_and_decrypts_data_cbc():
    key_id = "1234abcd-12ab-34cd-56ef-1234567890ab"
    kms_client = MagicMock()
    kms_mock = KmsMock(key_id)
    kms_client.generate_data_key.return_value = kms_mock.generate_data_key()
    kms_client.decrypt.return_value = kms_mock.decrypt()
    metadata = {
        "x-amz-key-v2": "key",
        "x-amz-iv": "IV",
        "x-amz-matdesc": json.dumps({"kms_cmk_id": key_id}),
        "x-amz-wrap-alg": "kms",
        "x-amz-cek-alg": "AES/CBC/PKCS5Padding",
        "x-amz-unencrypted-content-length": "890",
    }
    content = b'{"customer_id":12345}\n'
    encrypted, new_metadata = encrypt(BytesIO(content), metadata, kms_client)
    decrypted = decrypt(encrypted, new_metadata, kms_client)
    assert new_metadata == {
        "x-amz-key-v2": ANY,
        "x-amz-iv": ANY,
        "x-amz-matdesc": json.dumps({"kms_cmk_id": key_id}),
        "x-amz-wrap-alg": "kms",
        "x-amz-cek-alg": "AES/CBC/PKCS5Padding",
        "x-amz-unencrypted-content-length": "22",
    }
    assert new_metadata["x-amz-key-v2"] != "key"
    assert new_metadata["x-amz-iv"] != "IV"
    assert encrypted != content
    assert decrypted.read() == content
    kms_client.generate_data_key.assert_called_with(
        KeyId=key_id, EncryptionContext={"kms_cmk_id": key_id}, KeySpec="AES_256",
    )
    kms_client.decrypt.assert_called_with(
        CiphertextBlob=base64.b64decode(new_metadata["x-amz-key-v2"]),
        EncryptionContext={"kms_cmk_id": key_id},
    )


def test_it_encrypts_and_decrypts_data_gcm():
    key_id = "1234abcd-12ab-34cd-56ef-1234567890ab"
    kms_client = MagicMock()
    kms_mock = KmsMock(key_id)
    kms_client.generate_data_key.return_value = kms_mock.generate_data_key()
    kms_client.decrypt.return_value = kms_mock.decrypt()
    metadata = {
        "x-amz-key-v2": "key",
        "x-amz-iv": "IV",
        "x-amz-matdesc": json.dumps({"kms_cmk_id": key_id}),
        "x-amz-wrap-alg": "kms",
        "x-amz-cek-alg": "AES/GCM/NoPadding",
        "x-amz-unencrypted-content-length": "11",
        "x-amz-tag-len": "111",
    }
    content = b'{"customer_id":12345}\n'
    encrypted, new_metadata = encrypt(BytesIO(content), metadata, kms_client)
    decrypted = decrypt(encrypted, new_metadata, kms_client)
    assert new_metadata == {
        "x-amz-key-v2": ANY,
        "x-amz-iv": ANY,
        "x-amz-matdesc": json.dumps({"kms_cmk_id": key_id}),
        "x-amz-wrap-alg": "kms",
        "x-amz-cek-alg": "AES/GCM/NoPadding",
        "x-amz-unencrypted-content-length": "22",
        "x-amz-tag-len": "128",
    }
    assert new_metadata["x-amz-key-v2"] != "key"
    assert new_metadata["x-amz-iv"] != "IV"
    assert encrypted != content
    assert decrypted.read() == content
    kms_client.generate_data_key.assert_called_with(
        KeyId=key_id, EncryptionContext={"kms_cmk_id": key_id}, KeySpec="AES_256",
    )
    kms_client.decrypt.assert_called_with(
        CiphertextBlob=base64.b64decode(new_metadata["x-amz-key-v2"]),
        EncryptionContext={"kms_cmk_id": key_id},
    )

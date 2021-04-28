import base64
import json
import logging
import os
from io import BytesIO

from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import CBC
from cryptography.hazmat.primitives.padding import PKCS7

logger = logging.getLogger(__name__)

AES_BLOCK_SIZE = 128
ALG_CBC = "AES/CBC/PKCS5Padding"
ALG_GCM = "AES/GCM/NoPadding"
HEADER_ALG = "x-amz-cek-alg"
HEADER_KEY = "x-amz-key-v2"
HEADER_IV = "x-amz-iv"
HEADER_MATDESC = "x-amz-matdesc"
HEADER_TAG_LEN = "x-amz-tag-len"
HEADER_UE_CLENGHT = "x-amz-unencrypted-content-length"
HEADER_WRAP_ALG = "x-amz-wrap-alg"


def is_kms_cse_encrypted(s3_metadata):
    if HEADER_KEY in s3_metadata:
        if s3_metadata.get(HEADER_WRAP_ALG, None) != "kms":
            raise ValueError("Unsupported Encryption strategy")
        if s3_metadata.get(HEADER_ALG, None) not in [ALG_CBC, ALG_GCM]:
            raise ValueError("Unsupported Encryption algorithm")
        return True
    elif "x-amz-key" in s3_metadata:
        raise ValueError("Unsupported Amazon S3 Encryption Client Version")
    return False


def get_encryption_aes_key(key, kms_client):
    encryption_context = {"kms_cmk_id": key}
    response = kms_client.generate_data_key(
        KeyId=key, EncryptionContext=encryption_context, KeySpec="AES_256"
    )
    return (
        response["Plaintext"],
        encryption_context,
        base64.b64encode(response["CiphertextBlob"]).decode(),
    )


def get_decryption_aes_key(key, material_description, kms_client):
    return kms_client.decrypt(
        CiphertextBlob=key, EncryptionContext=material_description
    )["Plaintext"]


def encrypt(buf, s3_metadata, kms_client):
    """
    Method to encrypt an S3 object with KMS based Client-side encryption (CSE).
    The original object's metadata (previously used to decrypt the content) is
    used to infer some parameters such as the algorithm originally used to encrypt
    the previous version (which is left unchanged) and to store the new envelope,
    including the initialization vector (IV).
    """
    logger.info("Encrypting Object with CSE-KMS")
    content = buf.read()
    alg = s3_metadata.get(HEADER_ALG, None)
    matdesc = json.loads(s3_metadata[HEADER_MATDESC])
    aes_key, matdesc_metadata, key_metadata = get_encryption_aes_key(
        matdesc["kms_cmk_id"], kms_client
    )
    s3_metadata[HEADER_UE_CLENGHT] = str(len(content))
    s3_metadata[HEADER_WRAP_ALG] = "kms"
    s3_metadata[HEADER_KEY] = key_metadata
    s3_metadata[HEADER_ALG] = alg
    if alg == ALG_GCM:
        s3_metadata[HEADER_TAG_LEN] = str(AES_BLOCK_SIZE)
        result, iv = encrypt_gcm(aes_key, content)
    else:
        result, iv = encrypt_cbc(aes_key, content)
    s3_metadata[HEADER_IV] = base64.b64encode(iv).decode()
    return BytesIO(result), s3_metadata


def decrypt(file_input, s3_metadata, kms_client):
    """
    Method to decrypt an S3 object with KMS based Client-side encryption (CSE).
    The object's metadata is used to fetch the encryption envelope such as 
    the KMS key ID and the algorithm. 
    """
    logger.info("Decrypting Object with CSE-KMS")
    alg = s3_metadata.get(HEADER_ALG, None)
    iv = base64.b64decode(s3_metadata[HEADER_IV])
    material_description = json.loads(s3_metadata[HEADER_MATDESC])
    key = s3_metadata[HEADER_KEY]
    decryption_key = base64.b64decode(key)
    aes_key = get_decryption_aes_key(decryption_key, material_description, kms_client)
    content = file_input.read()
    decrypted = (
        decrypt_gcm(content, aes_key, iv)
        if alg == ALG_GCM
        else decrypt_cbc(content, aes_key, iv)
    )
    return BytesIO(decrypted)


# AES/CBC/PKCS5Padding


def encrypt_cbc(aes_key, content):
    iv = os.urandom(16)
    padder = PKCS7(AES.block_size).padder()
    padded_result = padder.update(content) + padder.finalize()
    aescbc = Cipher(AES(aes_key), CBC(iv)).encryptor()
    result = aescbc.update(padded_result) + aescbc.finalize()
    return result, iv


def decrypt_cbc(content, aes_key, iv):
    aescbc = Cipher(AES(aes_key), CBC(iv)).decryptor()
    padded_result = aescbc.update(content) + aescbc.finalize()
    unpadder = PKCS7(AES.block_size).unpadder()
    return unpadder.update(padded_result) + unpadder.finalize()


# AES/GCM/NoPadding


def encrypt_gcm(aes_key, content):
    iv = os.urandom(12)
    aesgcm = AESGCM(aes_key)
    result = aesgcm.encrypt(iv, content, None)
    return result, iv


def decrypt_gcm(content, aes_key, iv):
    aesgcm = AESGCM(aes_key)
    return aesgcm.decrypt(iv, content, None)

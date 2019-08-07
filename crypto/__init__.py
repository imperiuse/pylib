import uuid
import random
import string

from hashlib import md5
from base64 import urlsafe_b64decode
from base64 import urlsafe_b64encode

from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from Crypto.Util.Padding import pad, unpad


class AESCipher:
    def __init__(self, key):
        self.key = md5(key.encode('utf8')).digest()

    def encrypt(self, data):
        iv = get_random_bytes(AES.block_size)
        self.cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return urlsafe_b64encode(iv + self.cipher.encrypt(pad(data.encode('utf-8'),
            AES.block_size)))

    def decrypt(self, data):
        raw = urlsafe_b64decode(data)
        self.cipher = AES.new(self.key, AES.MODE_CBC, raw[:AES.block_size])
        return unpad(self.cipher.decrypt(raw[AES.block_size:]), AES.block_size)


def rand_string(cnt: int) -> str:
    return ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(cnt)])


def rand_digit(cnt: int) -> str:
    return ''.join([random.choice(string.digits) for _ in range(cnt)])


def hashed(data: str, salt: any = None) -> str:
    if salt is None:
        salt = uuid.uuid4().hex
        result = hashlib.sha256(salt.encode() + data.encode()).hexdigest() + ':' + salt
    else:
        result = hashlib.sha256(salt.encode() + data.encode()).hexdigest()
    return result


def check_hash(hashed_data: str, source_data: str, salt: any = None) -> bool:
    password, salt = hashed_data.split(':') if salt is None else hashed_data, salt
    return password == hashlib.sha256(salt.encode() + source_data.encode()).hexdigest()



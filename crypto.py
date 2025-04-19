from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import base64
import os

# Use a 16-byte key (128-bit AES)
SECRET_KEY =("MySuperSecretKeySilent").encode("utf-8")[:16]
IV = b"ThisIsInitVector"  # 16 bytes fixed IV (or random if you're storing it)

def encrypt_msg_id(msg_id: int) -> str:
    cipher = AES.new(SECRET_KEY, AES.MODE_CBC, IV)
    padded_data = pad(str(msg_id).encode("utf-8"), AES.block_size)
    encrypted = cipher.encrypt(padded_data)
    return base64.urlsafe_b64encode(encrypted).decode("utf-8")

def decrypt_msg_id(encrypted: str) -> int:
    cipher = AES.new(SECRET_KEY, AES.MODE_CBC, IV)
    decrypted = cipher.decrypt(base64.urlsafe_b64decode(encrypted))
    return int(unpad(decrypted, AES.block_size).decode("utf-8"))

from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA

message = 'To be signed'
key = RSA.import_key(open('./keys/keypair.pem').read())
h = SHA256.new()
h.update(message.encode())

signature = pkcs1_15.new(key).sign(h)

key = RSA.import_key(open('./keys/pub.pem').read())
h = SHA256.new()
h.update(message.encode())
try:
    pkcs1_15.new(key).verify(h, signature)
    print("The signature is valid.")
except (ValueError, TypeError):
    print("The signature is not valid.")

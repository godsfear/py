#!/python

from base64 import (
    b64encode,
    b64decode,
)

from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5
from Crypto.Hash import SHA256

def detect_by_bom(fname):
    import codecs
    with open(fname,'rb') as f:
        raw = f.read(4)
    for enc,boms in ('utf-8-sig',(codecs.BOM_UTF8,)),('utf-16',(codecs.BOM_UTF16_LE,codecs.BOM_UTF16_BE)),('utf-32',(codecs.BOM_UTF32_LE,codecs.BOM_UTF32_BE)):
        if any(raw.startswith(bom) for bom in boms): return enc
    return 'utf-8'

def readfile(filename):
    import os
    if os.path.exists(filename):
        with open(filename,'r',encoding=detect_by_bom(filename)) as f:
            try:
                data = f.read()
            except IOError:
                raise RuntimeError("Не могу прочитать: " + filename)
    else:
        raise RuntimeError("Файл не найден: " + filename)
    return data
    
def check_sign(public_key_loc,signature,data):
    keyfile = readfile(public_key_loc)
    rsakey = RSA.importKey(keyfile)
    signer = PKCS1_v1_5.new(rsakey)
    digest = SHA256.new()
    digest.update(data)
    if signer.verify(digest,signature):
        return True
    return False

def make_sign(private_key_loc,data,password=None):
    keyfile = readfile(private_key_loc)
    rsakey = RSA.importKey(keyfile,passphrase=password)
    digest = SHA256.new()
    digest.update(data)
    signer = PKCS1_v1_5.new(rsakey)
    sig = signer.sign(digest)
    return sig

def main():
    print(check_sign('./keys/pub.pem',make_sign('./keys/keypair.pem','Это тестовые данные!'.encode()),'Это тестовые данные!'.encode()))


if __name__ == '__main__':
    main()

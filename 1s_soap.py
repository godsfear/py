#!/python

from xfuncs import *
import requests

def main():
    head = {'Content-type':'text/xml; charset=utf-8','Accept':'text/xml'}
    body = """<?xml version="1.0" encoding="utf-8"?>
                <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:tns="http://www.1c.ru/docmng">
                  <soap:Body>
                    <tns:CalcLengthString>
                      <InputString>qwerty000</InputString>
                    </tns:CalcLengthString>
                  </soap:Body>
                </soap:Envelope>"""
    req = requests.post("http://localhost/test2/ws/wa_LengthString.1cws",auth=('Кобегенова Гульмира Жулдызбековна'.encode('utf-8').decode('latin1'),''),headers=head,data=body)
    print(req.status_code,req.reason)
    print(req.text)

if __name__ == '__main__':
    main()

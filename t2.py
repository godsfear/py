#!/python

import os
import sys
from lxml import etree

if __name__ == '__main__':
    fname = 'rez.xml'
    if os.path.exists(fname):
        with open(fname,'r',encoding='utf-8') as f:
            try:
                xml_txt = f.read()
            except IOError:
                print ("Could not read file:",fname)
                sys.exit(1)
    else:
        print("File not exist:",fname)
        sys.exit(1)
    doc = etree.XML(xml_txt)
    print(doc.xpath(r'/data/account/number')[0].text)

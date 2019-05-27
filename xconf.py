class Config(object):
    def __init__(self,**kwargs):
        self.data = {}
        for attribute,value in kwargs.items():
            setattr(self,attribute,value)

    def detect_by_bom():
        import codecs
        with open(self.fname,'rb') as f:
            raw = f.read(4)
        for enc,boms in ('utf-8-sig',(codecs.BOM_UTF8,)),('utf-16',(codecs.BOM_UTF16_LE,codecs.BOM_UTF16_BE)),('utf-32',(codecs.BOM_UTF32_LE,codecs.BOM_UTF32_BE)):
            if any(raw.startswith(bom) for bom in boms): return enc
        return 'utf-8'

    def load(self):
        import os
        if os.path.exists(self.fname):
            with open(self.fname,'r',encoding=self.detect_by_bom()) as f:
                try:
                    self.data = json.load(f)
                except IOError:
                    print ("Не могу прочитать файл конфигурации: ",self.fname)
        else:
            print("Файл конфигурации не найден: ",self.fname)

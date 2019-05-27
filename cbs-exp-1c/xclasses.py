class Config(object):
    def __init__(self,fname):
        import json,codecs
        self.data = {}
        self.fname = fname
        self.load()

    def __detect_by_bom(Config,fname):
        import json,codecs
        with open(fname,'rb') as f:
            raw = f.read(4)
        for enc,boms in ('utf-8-sig',(codecs.BOM_UTF8,)),('utf-16',(codecs.BOM_UTF16_LE,codecs.BOM_UTF16_BE)),('utf-32',(codecs.BOM_UTF32_LE,codecs.BOM_UTF32_BE)):
            if any(raw.startswith(bom) for bom in boms): return enc
        return 'utf-8'

    def load(self):
        import os,json
        if os.path.exists(self.fname):
            with open(self.fname,'r',encoding=self.__detect_by_bom(self.fname)) as f:
                try: self.data = json.load(f)
                except IOError: print ("Не могу прочитать файл конфигурации: ",self.fname)
        else: print("Файл конфигурации не найден: ",self.fname)

    def save(self):
        import json
        with open(self.fname,'w',encoding='utf-8') as f:
            try: json.dump(self.data,f,ensure_ascii=False)
            except IOError: print ("Не могу записать файл конфигурации: ",self.fname)

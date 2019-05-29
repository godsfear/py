#!/python
import sys,os,copy
from datetime import *
import json,re
import csv
from decimal import Decimal
import progressbar

def uqt(str,chr):
    return str[:1].replace(chr,'') + str[1:-1].replace(chr + chr,chr) + str[-1:].replace(chr,'') if str[:1] == str[-1:] else str

def add2date(idate,years=0,months=0,days=0):
    day = idate.day - 1 + days
    year = idate.year + years
    month = idate.month + months
    dyear,month = divmod(month,12)
    if month == 0:
        dyear -= 1
        month = 12
    rdate = datetime(year + dyear,month,1).date() + timedelta(days = day)
    return rdate

def loan_short(ibeg,iend):
    return (((iend - ibeg).days <= 365) or ((iend - ibeg).days == 366 and iend.day == ibeg.day))

def str2date(idate,fmt=None):
    idate = idate.strip()
    if not fmt is None:
        idate = datetime.strptime(idate,fmt)
    else:
        if idate == '':
            return None
        if idate.lower() in ['today','now']:
            return datetime.today().date()
        psplit = idate.find(' ')
        if psplit > 0:
            idate = idate[:psplit]
        psplit = idate.find('T')
        if psplit > 0:
            idate = idate[:psplit]
        try:
            year,month,day = map(int,idate.split('-'))
        except:
            try:
                year,month,day = map(int,idate.split('.'))
            except:
                try:
                    year,month,day = map(int,idate.split('/'))
                except:
                    return None
        if type(idate) is str:
            if year > 1700:
                idate = datetime(year,month,day).date()
            elif day < 100 and year < 100:
                idate = datetime(day + 2000,month,year).date()
            elif day > 1700:
                idate = datetime(day,month,year).date()
    return idate

def str2dec(idec):
    idec = idec.replace("'",'').replace('"','').replace(' ','')
    if idec == '' or idec.lower() == 'null':
        idec = 0
    else:
        if idec.find(',') >= 0:
            if idec.find('.') >= 0:
                idec = idec.replace(',','')
            else:
                if idec.count(',') > 1:
                    idec = idec.replace(',','')
                else:
                    idec = idec.replace(',','.')
        try:
            idec = Decimal(idec)
        except:
            idec = None
    return idec

def str2bool(ibool):
    ibool = ibool.replace("'",'').replace('"','').replace(' ','')
    ibool = ibool.lower()
    return (ibool in ['true','yes','1'])

def config(fname):
    if os.path.exists(fname):
        with open(fname,'r',encoding=detect_by_bom(fname)) as f:
            try:
                cfg = json.load(f)
            except IOError:
                print ("Не могу прочитать файл конфигурации: ",fname)
                return None
    else:
        print("Файл конфигурации не найден: ",fname)
        return None
    return cfg


def months(d2,d1):
    if d2 > d1:
        d1,d2 = d2,d1
    return (d1.year - d2.year) * 12 + d1.month - d2.month

def years(d2,d1):
    if d2 > d1:
        d1,d2 = d2,d1
    return d1.year - d2.year

def days(d2,d1,days30=False):
    if d2 > d1:
        d1,d2 = d2,d1
    from datetime import timedelta
    if days30:
        rez = 0
        for d in (d1 - timedelta(n) for n in range(int((d1 - d2).days + 1))):
            rez += 1
            if ((d - d2).days != 0) and (d.day == 1):
                rez = rez + 30 - int((d - timedelta(days=1)).day)
        return rez - 1
    else:
        return (d1 - d2).days
    
def prep_txt(txt):
    txt = txt.replace('\r','')
    txt = txt.replace('     ',' ')
    txt = txt.replace('    ',' ')
    txt = txt.replace('   ',' ')
    txt = txt.replace('  ',' ')
    txt = txt.replace('  ',' ')
    txt = txt.replace('\n ','\n')
    txt = txt.replace(' \n','\n')
    txt = txt.replace('` ','`')
    txt = txt.replace(' `','`')
    return txt

def printb(*objects,sep=' ',end='\n',file=sys.stdout):
    enc = file.encoding
    if enc == 'UTF-8':
        print(*objects,sep=sep,end=end,file=file)
    else:
        f = lambda obj: str(obj).encode(enc,errors='backslashreplace').decode(enc)
        print(*map(f,objects),sep=sep,end=end,file=file)

def install(package):
    """ Install packages """
    x = __file__.split('.')
    if len(x) > 1:
        if x[len(x) - 1] != 'py': return
    import subprocess
    rez = subprocess.Popen([sys.executable, "-m", "pip", "install", package],stdout = subprocess.PIPE,universal_newlines = True)
    for line in rez.stdout:
        if line.strip().startswith('Requirement already satisfied'):
            pass
        else:
            sys.stdout.write(line)

def connect(params):
    """ Connect to database server """
    conn = None
    prm = {'type':'','host':'','database':'','jdbc_class':'','jdbc_path':'','url':'','user':'','password':'','driver':''}
    for i in range(len(prm)):
        try:
            prm[list(prm.keys())[i]] = params[list(prm.keys())[i]]
        except:
            prm[list(prm.keys())[i]] = ''
    if prm['type'] == 'pgsql':
        conn = connect_pgsql(prm['host'],prm['database'],prm['user'],prm['password'])
    elif prm['type'] == 'jdbc':
        conn = connect_jdbc(prm['jdbc_class'],prm['url'],prm['user'],prm['password'],prm['jdbc_path'])
    elif prm['type'] == 'odbc':
        conn = connect_odbc(prm['driver'])
    return conn

def connect_pgsql(host,base,user,pasw):
    """ Connect to the PostgreSQL database server """
    install('psycopg2')
    import psycopg2
    conn = None
    try:
        conn = psycopg2.connect(host=host,database=base,user=user,password=pasw)
    except (Exception,RuntimeError) as error:
        print('Unable to connect!\n')
        print(error)
        sys.exit(1)
    return conn

def connect_jdbc(jdbc_class,url,user,pwd,jdbc_path):
    """ Connect to the JDBC database server """
    install('jaydebeapi')
    import jaydebeapi as jdbc
    conn = None
    try:
        conn = jdbc.connect(jdbc_class,[url,user,pwd],jdbc_path)
        conn.jconn.setAutoCommit(True)
    except (Exception,RuntimeError) as error:
        print('Unable to connect!\n')
        print(error)
        sys.exit(1)
    return conn

def connect_odbc(drv):
    """ Connect to the ODBC database server """
    install('pyodbc')
    import pyodbc
    conn = None
    d = ''
    driver = drv.split(';',1)
    try:
        d = re.search('{(.+?)}',driver[0]).group(1)
    except AttributeError:
        print('No driver info!')
        sys.exit(1)
    try:
        for x in pyodbc.drivers():
            if d in x:
                drv = x
                break
        conn = pyodbc.connect('DRIVER={' + drv + '};' + driver[1],autocommit=True)
    except (Exception,RuntimeError) as error:
        print('Unable to connect!\n')
        print(error)
        sys.exit(1)
    return conn

def query(conn,xquery):
    """ Query data from table """
    cur = conn.cursor()
    try:
        cur.execute(xquery)
    except (Exception,RuntimeError) as error:
        print(error)
    return cur

def insert(conn,tab,fld,val):
    """ Insert data into table """
    try:
        cur = conn.cursor()
        sql = "INSERT INTO " + tab + "(" + fld + ") VALUES(" + val + ");"
        cur.execute(sql)
        cur.close()
    except (Exception,RuntimeError) as error:
        print(error)

def txt2dict(fname,maps,dates,dateformat,decimals,bools,skip,unquot,quotechar,delimiter):
    if fname == '':
        return []
    objects = []
    if os.path.exists(fname):
        with open(fname,'r',encoding=detect_by_bom(fname)) as f:
            try:
                _csv = csv.reader(f,delimiter=delimiter,quotechar=quotechar)
            except IOError:
                print ("Ошибка чтения файла: ",fname)
                sys.exit(1)
            row_count = sum(1 for row in _csv)
            f.seek(0)
            cols = next(_csv,None)
            for c in cols:
                c = prep_txt(c).strip()
            with progressbar.ProgressBar(max_value=row_count) as bar:
                for k,line in enumerate(_csv):
                    obj = {}
                    cod = ''
                    for i,key in enumerate(cols):
                        if key in maps.keys():
                            cod = maps[key]
                        else:
                            continue
                        val = prep_txt(line[i]).strip()
                        if val in skip:
                            continue
                        if cod in dates:
                            try:
                                val = datetime.strptime(val,dateformat)
                            except:
                                val = str2date(val)
                        elif cod in bools:
                            val = str2bool(val)
                        elif cod in decimals:
                            val = str2dec(val)
                        elif cod in unquot:
                            val = uqt(val,'"')
                        obj.update({cod:val})
                    objects.append(obj)
                    bar.update(k)
    else:
        print("Файл не найден: ",fname)
        sys.exit(1)
    return objects

def x_str(val,dateformat):
    if type(val) is datetime or type(val) is date:
        rez = val.strftime(dateformat)
    elif not type(val) is str:
        rez = str(val)
    else:
        rez = val
    return rez

def cbs_fill(template,data,dateformat):
    if isinstance(template,list):
        for elem in template:
            cbs_fill(elem,data,dateformat)
    else:
        if isinstance(template,dict):
            for field in template:
                if (isinstance(template[field],list) and len(template[field]) > 0 and (isinstance(template[field][0],list) or isinstance(template[field][0],dict))) or isinstance(template[field],dict):
                    cbs_fill(template[field],data,dateformat)
                else:
                    if not (field in ['code','value','values','rows','rowId']):
                        if field in data.keys():
                            template[field] = x_str(data[field],dateformat)
                    else:
                        if field == 'code':
                            if any(k in template.keys() for k in ('values','value','rowId','rows')):
                                if template[field] in data.keys():
                                    if 'value' in template.keys():
                                        template['value'] = x_str(data[template[field]],dateformat)
                                    elif 'rowId' in template.keys():
                                        template['rowId'] = x_str(data[template[field]],dateformat)
                            else:
                                template[field] = data[field]

def cbs_group(obj,name,template,data,dateformat):
    exist = False
    prev = copy.deepcopy(template)
    cbs_fill(template,data,dateformat)
    if prev != template:
        for x in obj['groups']:
            if x['code'] == name:
                x['rows'].append(template[0])
                exist = True
        if not exist:
            obj['groups'].append({'code':name,'rows':template})

def cbs_nullify_callback(key,container):
    if not (key in ['code','value','values','rows','rowId']):
        if container[key] == '' or container[key] == 'None' or container[key] is None:
            del container[key]
    else:
        if key == 'code':
            if any(k in container.keys() for k in ('values','value','rowId','rows')):
                if 'value' in container.keys() and (container['value'] == '' or container['value'] == 'None'):
                    del container[key]
                    del container['value']
                if 'rowId' in container.keys() and container['rowId'] == '':
                    del container[key]
                    del container['rowId']

def nullify(container,callback=None,delete=False):
    for key in list(container):
        if isinstance(container,list) and not isinstance(key,(list,dict)):
            continue
        if isinstance(key,(list,dict)):
            nullify(key,callback=callback,delete=delete)
        elif key in container.keys() and isinstance(container[key],(dict,list)):
            nullify(container[key],callback=callback,delete=delete)
        elif callback is None:
            if container[key] == '' or container[key] == 'None' or container[key] is None:
                if delete:
                    container.pop(key,None)
                else:
                    container[key] = None
        else:
            callback(key,container)

def clean_empty(container):
    if not isinstance(container,(dict,list)):
        return container
    if isinstance(container,list):
        return [v for v in (clean_empty(v) for v in container) if v]
    return {k: v for k,v in ((k,clean_empty(v)) for k,v in container.items()) if v}

def cbs_clear(container):
    nullify(container,callback=cbs_nullify_callback,delete=True)
    return clean_empty(container)

def detect_by_bom(path,default='utf-8'):
    import codecs
    with open(path, 'rb') as f:
        raw = f.read(4)
    for enc,boms in ('utf-8-sig',(codecs.BOM_UTF8,)),('utf-16',(codecs.BOM_UTF16_LE,codecs.BOM_UTF16_BE)),('utf-32',(codecs.BOM_UTF32_LE,codecs.BOM_UTF32_BE)):
        if any(raw.startswith(bom) for bom in boms):
            return enc
    return default

def str_meta_comp(s1,s2,lang='ru',type='names'):
    import jellyfish
    from metaphone import doublemetaphone
    met1 = doublemetaphone(trans_lit(s1,lang,type))
    met2 = doublemetaphone(trans_lit(s2,lang,type))
    m = 0
    for m1 in met1:
        if m1 == '': continue
        for m2 in met2:
            if m2 == '': continue
            m = max(jellyfish.jaro_distance(m1,m2),m)
    return m

def trans_lit(txt,lang='ru',type='names'):
    if lang == 'ru':
        if type == 'names':
            abc = 'абвгдеёзийклмнопрстуфыэ'
            trn = 'abvgdeeziiklmnoprstufye'
            txt = txt.lower().translate(''.maketrans(abc,trn))
            txt = txt.replace('ж','zh').replace('х','kh').replace('ц','ts').replace('ч','ch').replace('ш','sh').replace('щ','shch').replace('ю','iu').replace('я','ia').replace('ъ','ie').replace('ь','')
    return txt

def nps_1c(nps):
    if len(nps) > 4:
        return (nps[0:4] + '.' + nps[4:])
    return nps

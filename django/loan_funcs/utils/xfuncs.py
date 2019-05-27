#!/python
import sys,os,copy
from datetime import *
import json,re
import csv
from decimal import Decimal

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
    if not fmt is None:
        idate = datetime.strptime(idate,fmt)
    else:
        try:
            year,month,day = map(int,idate.split('-'))
        except:
            try:
                year,month,day = map(int,idate.split('.'))
            except:
                try:
                    year,month,day = map(int,idate.split('/'))
                except:
                    idate = datetime.today().date()
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
    if idec.find(',') >= 0:
        if idec.find('.') >= 0:
            idec = idec.replace(',','')
        else:
            if idec.count(',') > 1:
                idec = idec.replace(',','')
            else:
                idec = idec.replace(',','.')
    idec = Decimal(idec)
    return idec

def str2bool(ibool):
    true = ['true','yes','1']
    ibool = ibool.replace("'",'').replace('"','').replace(' ','')
    ibool = ibool.lower()
    return (ibool in true)

def config(fname):
    if os.path.exists(fname):
        with open(fname,'r',encoding='utf-8') as f:
            try:
                cfg = json.load(f)
            except IOError:
                print ("Не могу прочитать файл конфигурации: ",fname)
                return None
    else:
        print("Файл конфигурации не найден: ",fname)
        return None
    return cfg


def months(d1,d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month
    
class _Period(object):
    def __init__(self,**kwargs):
        for attribute,value in kwargs.items():
            setattr(self,attribute,value)
    def years(self):
        return self.end.year - self.begin.year
    def months(self):
        return (self.end.year - self.begin.year) * 12 + self.end.month - self.begin.month
    def days(self):
        if self.days30:
            rez = 0
            for d in (self.end - timedelta(n) for n in range(int((self.end - self.begin).days + 1))):
                rez += 1
                if ((d - self.begin).days != 0) and (d.day == 1):
                    rez = rez + 30 - int((d - timedelta(days=1)).day)
            return rez
        else:
            return (self.end - self.begin).days
                
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

def txt2dict(fname,map,dates,dateformat,decimals,bools,skip,quotechar,delimiter):
    if fname == '':
        return []
    if os.path.exists(fname):
        with open(fname,'r',encoding='utf-8') as f:
            try:
                _csv = csv.reader(f,delimiter=delimiter,quotechar=quotechar)
                txt = ''
                for row in _csv:
                    txt = txt + ('\n' if txt != '' else '') + '`'.join(row)
            except IOError:
                print ("Ошибка чтения файла: ",fname)
                sys.exit(1)
    else:
        print("Файл не найден: ",fname)
        sys.exit(1)
    txt = re.split('\n',prep_txt(txt))
    objects = []
    k = 0
    for line in(txt):
        k += 1
        line = line.strip()
        if line == '':
            continue
        line = re.split('`',line)
        if k == 1:
            fline = line
            continue
        obj = {}
        cod = ''
        idx = -1
        for part in (fline):
            idx += 1
            try:
                cod = map[part]
            except:
                continue
            val = line[idx].strip()
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
            obj.update({cod:val})
        objects.append(obj)
    return objects

def _str(val,dateformat):
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
                            template[field] = _str(data[field],dateformat)
                    else:
                        if field == 'code':
                            if any(k in template.keys() for k in ('values','value','rowId','rows')):
                                if template[field] in data.keys():
                                    if 'value' in template.keys():
                                        template['value'] = _str(data[template[field]],dateformat)
                                    elif 'rowId' in template.keys():
                                        template['rowId'] = _str(data[template[field]],dateformat)
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
        if container[key] == '':
            del container[key]
    else:
        if key == 'code':
            if any(k in container.keys() for k in ('values','value','rowId','rows')):
                if 'value' in container.keys() and container['value'] == '':
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
            if container[key] == '':
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
    nullify(container,callback=cbs_nullify_callback)
    return clean_empty(container)

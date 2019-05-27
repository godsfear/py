#!/python

from xfuncs import *
from cbs import *
from datetime import *
install('requests')
import requests

def main():
    test = True
    fname = 'test.csv'
    service = 'create-customer'
    if len(sys.argv) > 1 and not ((sys.argv[1] == "") or (sys.argv[1] is None)):
        fname = sys.argv[1]
    if len(sys.argv) > 2 and not ((sys.argv[2] == "") or (sys.argv[2] is None)):
        service = sys.argv[2]
    who = 'ПКБ'
    cfg = config('migration.json')
    head = {'Content-type':'application/json','Accept':'text/plain'}
    url = cfg['target'][who]['URL'] + service
    flog = fname + '.log'
    log = open(flog,"w+",encoding='utf-8')
    xfile = ''
    if os.path.exists(fname):
        with open(fname,'r',encoding='utf-8') as f:
            try:
                xfile = f.read()
            except IOError:
                print ("Ошибка чтения файла: ",fname)
                sys.exit(1)
    else:
        print("Файл не найден: ",fname)
        sys.exit(1)
    xfile = xfile.replace('\r','')
    clients = re.split('\n',xfile)
    for client in clients:
        log.write('REQUEST:\n' + client + '\n')
        client = json.loads(client)
        if not test:
            req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=client)
            log.write('RESULT:\n' + req.status_code + ' ' + req.reason + '\n')
            log.write('ANSWER:\n' + req.text + '\n')

if __name__ == '__main__':
    main()

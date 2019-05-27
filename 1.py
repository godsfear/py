#!/usr/bin/env python3

import time
import threading
import postgresql

testtime = 10
nthreads = 4

def worker():
    nqueries = 0
    with postgresql.open('pq://eax@192.168.111.222/eax') as db:
        query = db.prepare("SELECT * FROM themes WHERE id = 1")
        starttime = time.time()
        while time.time() - starttime < testtime:
            query()
            nqueries = nqueries + 1
    print("Thread " + str(threading.get_ident()) + " - total " +
                      str(nqueries) + " queries executed")

for i in range(nthreads):
    t = threading.Thread(target = worker)
    t.start()
#!/python

#from db_api import *

if __name__ == '__main__':
    j = {}
    j.update({'1':'one'})
    j.update({'2':'two'})
    j.update({'0':'zero'})
    for e in (j):
        print(e,j[e])

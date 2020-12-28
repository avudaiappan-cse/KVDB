import sys
import os
import KVDB.kvdb
from filelock import FileLock
from threading import Thread


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


# Setting DB file
default_path = os.getcwd()
dbpath = sys.argv[1] if len(sys.argv) > 1 else default_path
file_path = dbpath + "\\localDB\\database.json"


try:
    db = KVDB.kvdb.KVDB(dbpath)
except Exception as e:
    print(e)
    sys.exit()
print("DB Connected Successfully at", dbpath + "\\localDB")
print("Welcome to Command Line Key-Value Data Store:")
while True:
    option = input()
    if option == "create":
        try:
            key = input("Enter Key: ")
            value = input("Enter Value: ")
            t1 = Thread(target=(db.set), args=(key, value,))
            t1.daemon = True
            t1.start()
        except Exception as e:
            pass
    elif option == "read":
        try:
            key = input("Enter Key: ")
            t2 = Thread(target=(db.get), args=(key,))
            t2.daemon = True
            t2.start()
        except Exception as e:
            print(bcolors.FAIL, e, bcolors.ENDC)
    elif option == "delete":
        try:
            key = input("Enter Key: ")
            t3 = Thread(target=(db.remove), args=(key,))
            t3.daemon = True
            t3.start()
        except Exception as e:
            print(bcolors.FAIL, e, bcolors.ENDC)
    elif option == "exit":
        db.releaseLock()
        break
    else:
        print(bcolors.FAIL + "Invalid option try with another" + bcolors.ENDC)

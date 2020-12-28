import json
import os
import time
import sys
from filelock import FileLock
from threading import Thread, activeCount


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


class KeySizeError(Exception):
    def __init__(self):
        print(bcolors.FAIL + "Key Size should be less than 32 characters" + bcolors.ENDC)


class ValueSizeError(Exception):
    def __init__(self):
        print(bcolors.FAIL + "Value Size should be less than 16 KB" + bcolors.ENDC)


class KeyExistError(Exception):
    def __init__(self):
        print(bcolors.FAIL + "Key Already Exist" + bcolors.ENDC)


class KeyNotExistError(Exception):
    def __init__(self):
        print(bcolors.FAIL + "Invalid Key" + bcolors.ENDC)


class MemorySizeExistError(Exception):
    def __init__(self):
        print(bcolors.FAIL + "Memory Exist" + bcolors.ENDC)


class InvalidValueType(Exception):
    def __init__(self):
        print(bcolors.FAIL + "Expected a JSON object Value" + bcolors.ENDC)


class PermissionDeniedException(Exception):
    def __init__(self, message):
        print(bcolors.FAIL + "Permission not allowed for " + message + bcolors.ENDC)


class KVDB:
    def __init__(self, path):
        '''
        Initial check whether the given db location
        is available for read and write
        '''
        if not os.access(path, os.R_OK):
            raise PermissionDeniedException("READ")
        if not os.access(path, os.W_OK):
            raise PermissionDeniedException("WRITE")

        self.dbpath = path + "\\localDB"
        self.path = path + "\\localDB\\database.json"
        '''
        Create a folder LocalDB for using our Database and
        opening a json file for CRD operations
        '''
        if not os.path.exists(self.dbpath):
            try:
                os.mkdir(self.dbpath)
            except NotImplementedError:
                raise NotImplementedError
        if not os.path.exists(self.path):
            try:
                fp = open(self.path, "w")
                fp.write(json.dumps({}, indent=4))
                fp.close()
            except:
                raise FileNotFoundError
        '''
        Acquiring a lock to prevent another client
        to prevent access the same database
        '''
        try:
            self.lock = FileLock(self.path + ".lock")
            self.lock.acquire()
        except Exception as fe:
            print(bcolors.WARNING +
                  "Another file using the DB, try with another database" + bcolors.ENDC)
            sys.exit()
        with open(self.path) as f:
            self.cache = json.load(f)

    def set(self, key, value):
        '''
        Avoiding unexpected delete of data
        reproducing file by cache data
        '''
        if not self.fileExist():
            self.saveFile(self.cache)
        if key == "" or value == "":
            print(bcolors.FAIL + "Empty Key or Value" + bcolors.ENDC)
            return
        # Key size should be less than or equal to 32 characters
        if len(key) > 32:
            raise KeySizeError
        # Size of value should be maximum of 16KB
        if len(str(value).encode("utf-8")) > 16000:
            raise ValueSizeError
        # Parsing the value to a python object, it parses the JSON object
        try:
            value = json.loads(str(value))
            if type(value) is not dict:
                raise InvalidValueType
        except json.JSONDecodeError:
            raise InvalidValueType
        # Checking size of the file does not exceed 1GB
        if os.stat(self.path).st_size + len(str(value).encode("utf-8")) + len(str(key).encode("utf-8")) > 1000000000:
            raise MemorySizeExistError
        data = self.cache
        if data.get(key) is None:
            '''
            Creating a new Data
            '''
            new_data = {}
            new_data["value"] = value
            new_data["Time-To-Live"] = int(round(time.time()
                                                 * 1000 * 60 * 60 * 24))  # Expires within a day
            data[key] = new_data
            if self.saveFile(data):
                self.cache[key] = new_data
            print(bcolors.OKGREEN + "Inserted Successfully!" + bcolors.ENDC)
        elif data.get(key) is not None:
            new_data = data.get(key)
            '''
            Data already exist,
            but checks whether the given key expires or not
            '''
            if new_data.get("Time-To-Live") == 0:
                new_data["value"] = value
                new_data["Time-To-Live"] = int(round(time.time()
                                                     * 1000 * 60 * 60 * 24))  # Expires within a day
                if self.saveFile(data):
                    self.cache[key] = new_data
                print(bcolors.OKGREEN + "Inserted Successfully!" + bcolors.ENDC)
            else:
                raise KeyExistError

    def get(self, key):
        '''
        Avoiding unexpected delete of data
        reproducing file by cache data
        '''
        if not self.fileExist():
            self.saveFile(self.cache)
        data = self.cache
        '''
        Checking for the given, return response as JSON
        '''
        if data.get(key) is None:
            raise KeyNotExistError
        exist_data = data.get(key)
        if exist_data.get("Time-To-Live") > 0:
            print(bcolors.OKBLUE + json.dumps(data.get(key)) + bcolors.ENDC)
        else:
            raise KeyNotExistError

    def remove(self, key):
        '''
        Avoiding unexpected delete of data
        reproducing file by cache data
        '''
        if not self.fileExist():
            self.saveFile(self.cache)
        data = self.cache
        '''
        Checking for the given, instead of deleting
        expiring the Time-To-Live to 0
        '''
        if data.get(key) is None:
            raise KeyNotExistError
        value = data.get(key)
        value["Time-To-Live"] = 0
        if self.saveFile(data):
            self.cache[key] = value
        else:
            raise Exception("Unable to save file")
            return
        print(bcolors.OKGREEN + "Deleted Successfully!" + bcolors.ENDC)

    def releaseLock(self):
        self.lock.release()

    def saveFile(self, data):
        try:
            fp = open(self.path, "w")
            fp.write(json.dumps(data))
            fp.close()
            return True
        except:
            raise Exception("Unable to save file")

    def fileExist(self):
        return os.path.exists(self.path)

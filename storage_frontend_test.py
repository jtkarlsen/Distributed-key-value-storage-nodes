import BaseHTTPServer
import sys
import getopt
import threading
import signal
import socket
import httplib
import random
import string
import time


class Node:
    def __init__(self, hostname, port):
        self.hostname = hostname
        self.port = port

    def get_hostname(self):
        return self.hostname

    def get_port(self):
        return self.port

    def get_print(self):
        return self.hostname + ":" + str(self.port)

        
class StorageServerTest:

    testsToRun = 100
    i = 0

    def __init__(self):
        self.pairs = dict()
        for i in range(self.testsToRun):
            while True:
                key, value = self.generateKeyValuePair()
                if key not in self.pairs:
                    break
            self.pairs[key] = value

        
    def generateKeyValuePair(self):
        key = ''
        value = ''
        
        for i in range(random.randint(10, 20)):
            key += random.choice(string.letters)
        
        for i in range(random.randint(20,40)): 
            value += random.choice('1234567890')
        
        return key, value

    def run(self):
        start_time = time.time()
        keyValuePairs = dict()
        
        # Generate random unique key, value pairs
        for i in range(self.testsToRun):
            while True:
                key, value = self.generateKeyValuePair()
                if key not in keyValuePairs:
                    break
            keyValuePairs[key] = value
        
        # Call put to insert the key/value pairs
        for key, value in keyValuePairs.iteritems():
            if self.putTestObject(key, value) != True:
                print "Error putting", key, value
                return False
        i = 0
        # Validate that all key/value pairs are found
        for key, value in keyValuePairs.iteritems():
            i+=1
            if self.getTestObject(key, value) != True:
                print "Error getting", key, value
                print "index: " + str(i) + ", of " + str(len(keyValuePairs))
                return False

        elapsed_time = time.time() - start_time
        print "Time: " + str(elapsed_time)
        
        return True
    
    def put(self):
        # Call put to insert the key/value pairs
        for key, value in self.pairs.iteritems():
            if self.putTestObject(key, value) != True:
                print "Error putting", key, value
                return False
    def get(self):
        i=0
        # Validate that all key/value pairs are found
        for key, value in self.pairs.iteritems():
            i+=1
            if self.getTestObject(key, value) != True:
                print "Error getting", key, value
                print "index: " + str(i) + ", of " + str(len(self.pairs))
                return False
    def getTestObject(self, key, value):
        print "GET(key, value):", key, value
        node = front
        try:
            conn = httplib.HTTPConnection(node.get_hostname(), node.get_port())
            conn.request("GET", key)
            response = conn.getresponse()
            if response.status != 200:
                print "response.status != 200: " + response.reason
                return False
                
            retrievedValue = response.read()
        except:
            print "Unable to send GET request"
            return False
        
        if value != retrievedValue:
            print "Value is not equal to retrieved value", value, "!=", retrievedValue
            return False
    
        return True

    
    def putTestObject(self, key, value):
        print "PUT(key, value):", key, value
        node = front
        try:
            conn = httplib.HTTPConnection(node.get_hostname(), node.get_port())
            conn.request("PUT", key, value)
            response = conn.getresponse()
            
        except:
            return False

        return True

def main():
    global front

    try:
      print 'Argv: ', sys.argv[1:]
      optlist, args = getopt.getopt(sys.argv[1:], 'x', ['frontend='])
      print 'Options: ', optlist
    
    except getopt.GetoptError:
      print sys.argv[0] + ' [--replicas=(number of nodereplications)] --port=(portnumber) --controller=(hostname.local:port)'
      sys.exit(2)

    for opt, arg in optlist:
        if opt in ("-frontend", "--frontend"):
            tempfront = str(arg)
            li = tempfront.split(':')
            front = Node(li[0], li[1])


     
    tests = StorageServerTest()

    

    

    while True:
        key = raw_input()
        if key == "put":
            tests.put()
        if key == "get":
            tests.get()
        if key == "run":
            tests.start()

    

if __name__ == '__main__': main()
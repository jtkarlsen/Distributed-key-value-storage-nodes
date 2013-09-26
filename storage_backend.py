import bisect
import md5
import socket

import sys

import BaseHTTPServer
import getopt
import threading
import signal
import socket
import httplib
import random
import string


MAX_CONTENT_LENGHT = 1024   # Maximum length of the content of the http request (1 kilobyte)
MAX_STORAGE_SIZE = 104857600  # Maximum total storage allowed (100 megabytes)
NUMBER_OF_REPLICAS = 200

httpdServeRequests = True
storageBackendNodes = list()

#working class hero
class ConsistentHash:

    def __init__(self):
        self.num_replicas = NUMBER_OF_REPLICAS
        hash_tuples = [(n,k,my_hash(str(n)+"_"+str(k))) \
                       for n in storageBackendNodes \
                       for k in range(self.num_replicas)]
        # Sort the hash tuples based on just the hash values
        hash_tuples.sort(lambda x,y: cmp(x[2],y[2]))
        self.hash_tuples = hash_tuples
        print hash_tuples

    def get_machine(self,key):
        #Returns the number of the machine which key gets sent to.
        h = my_hash(key)
        # edge case where we cycle past hash value of 1 and back to 0.
        if h > self.hash_tuples[-1][2]: return self.hash_tuples[0][0]
        hash_values = map(lambda x: x[2],self.hash_tuples)
        index = bisect.bisect_left(hash_values,h)
        return self.hash_tuples[index][0]

#md5 hash function
def my_hash(key):
    #my_hash(key) returns a hash in the range [0,1).
    return (int(md5.new(key).hexdigest(),16) % 1000000)/1000000.0






class StorageServer:
  
  def __init__(self):
    self.map = dict()
    self.size = 0
    self.portnumber = 8000

    try:
      print 'Argv: ', sys.argv[1:]
      optlist, args = getopt.getopt(sys.argv[1:], 'x', ['replicas='])
      print 'Options: ', optlist
    
    except getopt.GetoptError:
      print sys.argv[0] + ' [--replicas=(number of nodereplications)] compute-1-1 compute-1-1 ... compute-N-M'
      sys.exit(2)

    if len(args) <= 0:
      print sys.argv[0] + ' [--replicas=(number of nodereplications)] compute-1-1 compute-1-1 ... compute-N-M'
      sys.exit(2)

    for opt, arg in optlist:
        if opt in ("-replicas", "--replicas"):
            NUMBER_OF_REPLICAS = int(arg)


    # Nodelist
    for node in args:
      storageBackendNodes.append(node)
      print "Added", node, "to the list of nodes"

    self.name = socket.gethostname()
    self.ch = ConsistentHash()
    print "Format:"
    print "(machine,replica,hash value):"
    found = False
    for (n,k,h) in self.ch.hash_tuples: 
      print "(%s,%s,%s)" % (n,k,h)
      if n == self.name: found = True
    if found == False: 
      print "This computer is not in the node list. please fix this. Exiting now."
      sys.exit()

  
  def sendGET(self, key):
    node = self.ch.get_machine(key)
    if self.name == node:
      return self.map[key]

    else:
      try:
        conn = httplib.HTTPConnection(node, self.portnumber)
        conn.request("GET", key)
        response = conn.getresponse()
        if response.status != 200:
          return False
        
        return response.read()

      except:
        print "Unable to send GET request"
        return False
    
  def sendPUT(self, key, value, size):
    self.size = self.size + size
    node = self.ch.get_machine(key)

    if self.name == node:
      self.map[key] = value

    else:
      try:
        conn = httplib.HTTPConnection(node, self.portnumber)
        conn.request("PUT", key, value)
        response = conn.getresponse()
      except:
        return False
    
    print "Used storage in bytes: ", self.size
    return True






class HttpHandler(BaseHTTPServer.BaseHTTPRequestHandler):
  global storage 
  storage = StorageServer()
  
  # Returns the 
  def do_GET(self):
    key = self.path
    value = storage.sendGET(key)
    
    if value is None:
      self.sendErrorResponse(404, "Key not found")
      return
    
    # Write header
    self.send_response(200)
    self.send_header("Content-type", "application/octet-stream")
    self.end_headers()
    
    # Write Body
    self.wfile.write(value)
    
  def do_PUT(self):
    contentLength = int(self.headers['Content-Length'])
    
    if contentLength <= 0 or contentLength > MAX_CONTENT_LENGHT:
      self.sendErrorResponse(400, "Content body to large")
      return
    
    storage.size += contentLength
    if storage.size > MAX_STORAGE_SIZE:
      self.sendErrorResponse(400, "Storage server(s) exhausted")
      return
    
    # Forward the request to another backend server
    storage.sendPUT(self.path, self.rfile.read(contentLength), contentLength)
    
    self.send_response(200)
    self.send_header("Content-type", "text/html")
    self.end_headers()
    
  def sendErrorResponse(self, code, msg):
    self.send_response(code)
    self.send_header("Content-type", "text/html")
    self.end_headers()
    self.wfile.write(msg)
    






class HTTPServer(BaseHTTPServer.HTTPServer):
  
  def server_bind(self):
    BaseHTTPServer.HTTPServer.server_bind(self)
    self.socket.settimeout(1)
    self.run = True

  def get_request(self):
    while self.run == True:
      try:
        sock, addr = self.socket.accept()
        sock.settimeout(None)
        return (sock, addr)
      except socket.timeout:
        if not self.run:
          raise socket.error

  def stop(self):
    self.run = False

  def serve(self):
    while self.run == True:
      self.handle_request()





def main():
    httpserver_port = 8000

    # Start the webserver which handles incomming requests
    try:
      httpd = HTTPServer(("",httpserver_port), HttpHandler)
      server_thread = threading.Thread(target = httpd.serve)
      server_thread.daemon = True
      server_thread.start()
    
      def handler(signum, frame):
        print "Stopping http server..."
        httpd.stop()
      signal.signal(signal.SIGINT, handler)
    
    except:
      print "Error: unable to http server thread"

    server_thread.join(100)


if __name__ == '__main__': main()
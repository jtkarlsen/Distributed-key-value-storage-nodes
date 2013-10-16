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
from SocketServer import ThreadingMixIn


MAX_CONTENT_LENGHT = 1024   # Maximum length of the content of the http request (1 kilobyte)
MAX_STORAGE_SIZE = 104857600  # Maximum total storage allowed (100 megabytes)
NUMBER_OF_REPLICAS = 300

httpdServeRequests = True

socket.setdefaulttimeout(None)

global storageBackendNodes


#md5 hash function
def my_hash(key):
    #my_hash(key) returns a hash in the range [0,1).
    return (int(md5.new(key).hexdigest(),16) % 1000000)/1000000.0

def print_nodes():
  for node in storageBackendNodes:
    print node.get_print()

def remove_node(node):
  tempnode = None
  for node_in_list in storageBackendNodes:
    if node_in_list.get_print() == node.get_print():
      tempnode = node_in_list
  storageBackendNodes.remove(tempnode)



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

#working class hero
class ConsistentHash:

    def __init__(self):
        global storageBackendNodes
        self.num_replicas = NUMBER_OF_REPLICAS
        hash_tuples = [(n,k,my_hash(str(n.get_print())+"_"+str(k))) \
                       for n in storageBackendNodes \
                       for k in range(self.num_replicas)]
        # Sort the hash tuples based on just the hash values
        hash_tuples.sort(lambda x,y: cmp(x[2],y[2]))
        self.hash_tuples = hash_tuples

    def get_machine(self,key):
        #Returns the number of the machine which key gets sent to.
        h = my_hash(key)
        # edge case where we cycle past hash value of 1 and back to 0.
        if h > self.hash_tuples[-1][2]: return self.hash_tuples[0][0]
        hash_values = map(lambda x: x[2],self.hash_tuples)
        index = bisect.bisect_left(hash_values,h)
        return self.hash_tuples[index][0]

    def add_machine(self, node):
        storageBackendNodes.append(node)
        hash_tuples = [(n,k,my_hash(str(n.get_print())+"_"+str(k))) \
                       for n in storageBackendNodes \
                       for k in range(self.num_replicas)]
        # Sort the hash tuples based on just the hash values
        hash_tuples.sort(lambda x,y: cmp(x[2],y[2]))
        self.hash_tuples = hash_tuples

    def remove_machine(self, node):
      remove_node(node)
      hash_tuples = [(n,k,my_hash(str(n.get_print())+"_"+str(k))) \
                      for n in storageBackendNodes \
                      for k in range(self.num_replicas)]
      # Sort the hash tuples based on just the hash values
      hash_tuples.sort(lambda x,y: cmp(x[2],y[2]))
      self.hash_tuples = hash_tuples




class StorageServer:
  
  def __init__(self):
    global storageBackendNodes
    self.map = dict()
    self.size = 0

    self.name = socket.gethostname()
    self.node = Node(self.name, portnumber)
    storageBackendNodes.append(self.node)

    self.ch = ConsistentHash()
    print "Format:"
    print "(machine,replica,hash value):"
    found = False
    #for (n,k,h) in self.ch.hash_tuples: 
      #print "(%s,%s,%s)" % (n.get_print(),k,h)

  def getInf(self):
    return " - Load: " + str(self.size) + " bytes, Keys: " + str(len(self.map))

  def distributeKeys(self):
    print "Start distributing"
    if not storageBackendNodes:
      print "Last node. Shutting down without distributing data."
      return False
    temp = list()
    for key in self.map:
      node = self.ch.get_machine(key)
      if node.get_print() != self.node.get_print():
        self.sendPUT(key, self.map[key], len(self.map[key]))
        temp.append(key)

    for key in temp:
      self.size -= len(self.map[key])
      del self.map[key]
    print "Done distributing! New size=" + str(self.size)

  def setPort(self, port):
    self.node = Node(self.name, port)
    storageBackendNodes[0] = Node(self.name, port)

  def sendJOIN(self):
    try:
      conn = httplib.HTTPConnection(controller.get_hostname(), controller.get_port())
      conn.request("PUT", "add", self.node.get_print())
      response = conn.getresponse()
    except socket.timeout:
      print "TIMEOUT"
      return False
    except socket.error:
      print "SOCKET ERROR"
      return False
    except:
      print "Unable to send JOIN request"
      return False
    return True

  def sendLEAVE(self):
    try:
      conn = httplib.HTTPConnection(controller.get_hostname(), controller.get_port())
      conn.request("PUT", "rem", self.node.get_print())
      response = conn.getresponse()
    except:
      print "Unable to send LEAVE request"
      return False

    self.nodeLeaving(self.node)

    return True

  def nodeJoining(self, node):
    self.ch.add_machine(node)
    print "Node joined: "+node.get_print()
    #print_nodes()
    self.distributeKeys()

  def listOfNodesJoining(self, nodes):
    print "Adding list of nodes"
    for node in nodes:
      self.ch.add_machine(node)
    self.distributeKeys()

  def nodeLeaving(self, node):
    self.ch.remove_machine(node)
    print_nodes()
    self.distributeKeys()
    if node.get_print() == self.node.get_print():
      print "Stopping server.."
      httpd.stop()


  
  def sendGET(self, key):
    node = self.ch.get_machine(key)
    hostname = node.get_hostname()
    port = node.get_port()
    if (self.name == hostname) and (portnumber == port):
      try:
        return self.map[key]
      except:
        return False

    try:
      conn = httplib.HTTPConnection(hostname, port)
      conn.request("GET", key)
      response = conn.getresponse()
      if response.status != 200:
        return False
        
      return response.read()

    except:
      print "Unable to send GET request"
      return False
    
  def sendPUT(self, key, value, size):
    node = self.ch.get_machine(key)
    hostname = node.get_hostname()
    port = node.get_port()
    if (self.name == hostname) and (portnumber == port):
      try:
        self.size += size
        self.map[key] = value
      except:
        print "Could not put"
        return False
    else:
      try:
        conn = httplib.HTTPConnection(hostname, port)
        conn.request("PUT", key, value)
        response = conn.getresponse()
      except:
        return False
    
    return True






class HttpHandler(BaseHTTPServer.BaseHTTPRequestHandler):
  
  
  # Returns the 
  def do_GET(self):
    key = self.path
    if key == "inf":
      value = storage.getInf()
    else:
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
    
    if storage.size > MAX_STORAGE_SIZE:
      self.sendErrorResponse(400, "Storage server(s) exhausted")
      return
    
    if self.path == "add":
      new_node = self.rfile.read(contentLength)
      myparams = new_node.split(':')
      node = Node(myparams[0], myparams[1])
      storage.nodeJoining(node)

    elif self.path == "addlist":
      res = self.rfile.read(contentLength)
      nodes = res.split(',')
      tempnodes = list()
      for node in nodes:
        myparams = node.split(':')
        tempnode = Node(myparams[0], myparams[1])
        tempnodes.append(tempnode)
      storage.listOfNodesJoining(tempnodes)


    elif self.path == "rem":
      new_node = self.rfile.read(contentLength)
      myparams = new_node.split(':')
      node = Node(myparams[0], myparams[1])
      storage.nodeLeaving(node)

    else:
      storage.sendPUT(self.path, self.rfile.read(contentLength), contentLength)
    
    self.send_response(200)
    self.send_header("Content-type", "text/html")
    self.end_headers()
    
  def sendErrorResponse(self, code, msg):
    self.send_response(code)
    self.send_header("Content-type", "text/html")
    self.end_headers()
    self.wfile.write(msg)
    






class HTTPServer(ThreadingMixIn, BaseHTTPServer.HTTPServer):
  
  def server_bind(self):
    BaseHTTPServer.HTTPServer.server_bind(self)
    self.socket.settimeout(None)
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

def runServer(port, iterations):
  global httpd
  global portnumber
  # Start the webserver which handles incomming requests
  portnumber = port
  storage.setPort(port)
  #if iterations > 1:
  #  sys.exit(2)
  try:
      httpd = HTTPServer(("",port), HttpHandler)
      httpd.socket.settimeout(None)
      server_thread = threading.Thread(target = httpd.serve)
      server_thread.daemon = True
      server_thread.start()
    
      def handler(signum, frame):
        print "Preparing shutdown."
        storage.sendLEAVE()
        print "Shutting down now."
        httpd.stop()
      signal.signal(signal.SIGINT, handler)

      #Send join request
      storage.sendJOIN()

      server_thread.join()
    
  except:
    runServer(int(port)+1, int(iterations)+1)


def main():
    global httpd
    global server_thread
    global portnumber
    global storage
    global controller
    global storageBackendNodes
    global iterations
    iterations = 0
    storageBackendNodes = list()

    try:
      print 'Argv: ', sys.argv[1:]
      optlist, args = getopt.getopt(sys.argv[1:], 'x', ['port=', 'controller='])
      print 'Options: ', optlist
    
    except getopt.GetoptError:
      print sys.argv[0] + ' --port=(portnumber) --controller=(hostname.local:port)'
      sys.exit(2)

    for opt, arg in optlist:
        if opt in ("-port", "--port"):
            portnumber = int(arg)
        if opt in ("-controller", "--controller"):
            tempcontroller = str(arg)
            li = tempcontroller.split(':')
            controller = Node(li[0], li[1])

     
    storage = StorageServer()

    runServer(portnumber, iterations)

    

if __name__ == '__main__': main()
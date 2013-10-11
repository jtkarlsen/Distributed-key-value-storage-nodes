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

import time


MAX_CONTENT_LENGHT = 1024   # Maximum length of the content of the http request (1 kilobyte)
MAX_STORAGE_SIZE = 104857600  # Maximum total storage allowed (100 megabytes)

httpdServeRequests = True
storageBackendNodes = list()

def remove_node(node):
  tempnode = None
  for node_in_list in storageBackendNodes:
    if node_in_list.get_print() == node.get_print():
      tempnode = node_in_list
  storageBackendNodes.remove(tempnode)

def print_nodes():
  for node in storageBackendNodes:
    print node.get_print()

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

class ControllerServer:

  def sendADD(self, new_node):

    for node in storageBackendNodes:
      try:
        conn = httplib.HTTPConnection(node.get_hostname(), node.get_port())
        conn.request("PUT", "add", new_node.get_print())
        response = conn.getresponse()
      except:
        print "Unable to send ADD request"

      try:
          conn = httplib.HTTPConnection(new_node.get_hostname(), new_node.get_port())
          conn.request("PUT", "add", node.get_print())
          response = conn.getresponse()
      except:
          print "Unable to send LIST request"    
    
  def sendREMOVE(self, old_node):

    for node in storageBackendNodes:
      try:
        conn = httplib.HTTPConnection(node.get_hostname(), node.get_port())
        conn.request("PUT", "rem",old_node.get_print())
        response = conn.getresponse()
      except:
        print "Unable to send REMOVE request"

  def sendSafeREMOVE(self, old_node):
    removed = False
    for node in storageBackendNodes:
      if node.get_print() == old_node.get_print():
        remove_node(old_node)
        removed = True

    for node in storageBackendNodes:
      try:
        conn = httplib.HTTPConnection(node.get_hostname(), node.get_port())
        conn.request("PUT", "rem",old_node.get_print())
        response = conn.getresponse()
      except:
        print "Unable to send REMOVE request"

    if removed == True:
      try:
        conn = httplib.HTTPConnection(old_node.get_hostname(), old_node.get_port())
        conn.request("PUT", "rem",old_node.get_print())
        response = conn.getresponse()
      except:
        print "Unable to send REMOVE request"


class HttpHandler(BaseHTTPServer.BaseHTTPRequestHandler):
  global controller 
  controller = ControllerServer()
  
  # Returns the 
  def do_PUT(self):
    contentLength = int(self.headers['Content-Length'])
    new_node = self.rfile.read(contentLength)

    
    if new_node is None:
      self.sendErrorResponse(404, "not found")
      return

    myparams = new_node.split(':')
    node = Node(myparams[0], myparams[1])

    if self.path == "add":
      value = controller.sendADD(node)
      storageBackendNodes.append(node)


    if self.path == "rem":
      remove_node(node)
      value = controller.sendREMOVE(node)

    
    # Write header
    self.send_response(200)
    self.send_header("Content-type", "application/octet-stream")
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
    SELF_PORT = 8000
    SELF_HOSTNAME = socket.gethostname()

    print "Started! Waiting for nodes."
    print "My address is "+SELF_HOSTNAME+":"+str(SELF_PORT)
    # Start the webserver which handles incomming requests
    try:
      httpd = HTTPServer(("",SELF_PORT), HttpHandler)
      server_thread = threading.Thread(target = httpd.serve)
      server_thread.daemon = True
      server_thread.start()
    
      def handler(signum, frame):
        print "Stopping http server..."
        httpd.stop()
      signal.signal(signal.SIGINT, handler)

      while True:
        print_nodes()
        print "\nEnter a node to kill (hostname.local:port OR index):"
        key = raw_input()

        if key.isdigit() == True:
          if len(storageBackendNodes) > int(key):
            print "Killing node with index "+str(key)
            node = storageBackendNodes[int(key)]
            ControllerServer().sendSafeREMOVE(node)
            print "Node killed, removing from list"
        for node in storageBackendNodes:
          if node.get_print() == key:
            ControllerServer().sendSafeREMOVE(node)
    except:
      print "Error: unable to http server thread"

    #server_thread.join(100)


if __name__ == '__main__': main()
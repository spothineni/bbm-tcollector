#!/usr/bin/python

import errno
import urllib
import urllib2
try:
  import json
except ImportError:
  json = None  # Handled gracefully in main.  Not available by default in <2.6
import socket
import sys
import time

from collectors.lib import utils

HOST="localhost"
COLLECTION_INTERVAL = 15  # seconds
DEFAULT_TIMEOUT = 5.0    # seconds

def request(uri):
  """Does a GET request of the given uri on the given HTTPConnection."""
  try:
    response = urllib2.urlopen(uri)
    json_object = json.load(response)
    return json_object
  except:
    return {}

def worker_stats():
  return request("http://%s:25000/jsonmetrics" % (HOST))

def statestore_stats():
  return request("http://%s:25010/jsonmetrics" % (HOST))

def catalog_stats():
  return request("http://%s:25020/jsonmetrics" % (HOST))

def process_stats(stats):
  output = {}
  for k, v in stats.items():
    if isinstance(v, (int, long, float, complex)):
      output[k] = v 
    if isinstance(v, dict):
      for sk, sv in v.items():
        if isinstance(sv, (int, long, float, complex)):
          output["%s.%s" %(k, sk)] = sv 

  return output

def output_stats(service,ts,stats):
  for k, v in process_stats(stats).items():
    print "impala.%s %s %s service=%s" % (k , ts, v , service)
  return 

def main(argv):
  utils.drop_privileges()
  socket.setdefaulttimeout(DEFAULT_TIMEOUT)
  if json is None:
    err("This collector requires the `json' Python module.")
    return 1

  while True:
    ts = int(time.time())
    output_stats("impalad",ts,worker_stats())
    output_stats("statestored",ts,statestore_stats())
    output_stats("catalogd",ts,catalog_stats())
    time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
  sys.exit(main(sys.argv))

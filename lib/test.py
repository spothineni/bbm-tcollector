import time
import signal
import sys
#from bbmTcollectorUtils import enqueue_process, start_tomcat_collector, QUEUE_FINISHED
#from bbmTcollectorUtils import enqueue_process, start_jvm_collector, QUEUE_FINISHED
#from bbmTcollectorUtils import enqueue_process, start_c3p0_collector, QUEUE_FINISHED

from bbm import QUEUE_FINISHED
from bbm.jmx import start_jmx_collector
from bbm.jvm import jvm_collector
from bbm.tomcat import tomcat_collector
from bbm.c3p0 import c3p0_collector


#from threading  import Thread
#
#def tester():
#  print >>sys.stderr, "timeout"
#  return True
#
##t = Thread(target=enqueue_process, args=(q, "tail", ["--lines","0","-f", "/var/log/syslog"],1000, tester))
#t.daemon = True # thread dies with the program
#t.start()

signal.signal(signal.SIGCHLD, signal.SIG_IGN)

def renamer(v):
    return v

q = start_jmx_collector(1, "6031", jvm_collector)

while True:
  line = q.get()
  if line != QUEUE_FINISHED:
    print >>sys.stdout, line
  else:
    break


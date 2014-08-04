import os
import re
import subprocess
import sys
import time
import select
import atexit
from threading  import Thread
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # python 3.x
import pyinotify
from bbm import QUEUE_FINISHED, TSDBMetricData

ON_POSIX = 'posix' in sys.builtin_module_names
POLL_TIMEOUT_MS = 300000 # 5 minutes

def onTimeOutTrue():
    return True

def enqueue_process(queue, cmd, args, timeout=POLL_TIMEOUT_MS, onTimeOut=onTimeOutTrue,mapFunc=None):
    # Get initial log file name
    command = [ cmd ] + args

    try:
        proc1 = subprocess.Popen(command, stdout=subprocess.PIPE, bufsize=0, close_fds=ON_POSIX)
        def forceclose():
            try:
                proce1.terminate()
            except Exception as e:
                return

        atexit.register(forceclose)

        poll_obj = select.poll()
        poll_obj.register(proc1.stdout, select.POLLIN)
        while(True):
            poll_result = poll_obj.poll(timeout)
            if poll_result:
                line = proc1.stdout.readline()
                if not line:
                    break
                if mapFunc == None:
                    queue.put(line.rstrip("\n"))
                else:
                    try:
                        data = mapFunc(line.rstrip("\n"))
                        if isinstance(data, list):
                            for d in data:
                                queue.put(d)
                        else:
                            queue.put(data)
                    except Exception as e:
                         print >>sys.stderr, "mapFunc failed:"
                         print >>sys.stderr, e 
            else:
                if not onTimeOut():
                    break
    except Exception as e:
        print >>sys.stderr, "Launching %s failed:" % cmd 
        print >>sys.stderr, e 
    finally:
        queue.put(QUEUE_FINISHED)
        proc1.terminate()

def start_process_collector(cmd, args, timeout=POLL_TIMEOUT_MS, onTimeOut=onTimeOutTrue,mapFunc=None):
    q = Queue()

    t = Thread(target=enqueue_process, args=(cmd, args ,timeout,onTimeOut,mapFunc))
    t.daemon = True # thread dies with the program
    t.start()

    return q


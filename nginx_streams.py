#!/usr/bin/python
from collectors.lib import utils
import subprocess
import select
import signal
import os
import sys
import datetime
import time
import string
import urlparse
import glob
from threading  import Thread
import pyinotify 


try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # python 3.x

ON_POSIX = 'posix' in sys.builtin_module_names

QUEUE_FINISHED = object()
POLL_TIMEOUT_MS = 300000 # 5 minutes

def enqueue_file(queue, filename):
    # Get initial log file name
    command = ['tail', '-f'] + [ filename ]
    
    proc1 = subprocess.Popen(command, stdout=subprocess.PIPE, bufsize=1, close_fds=ON_POSIX)

    poll_obj = select.poll()
    poll_obj.register(proc1.stdout, select.POLLIN)   
    while(True):
        poll_result = poll_obj.poll(POLL_TIMEOUT_MS)
        if poll_result:
             line = proc1.stdout.readline()
             if line is None:
               break
             queue.put(line)
        else:
          # We've timed out waiting for input
          # If this file is not the current active log file, we just quite.
          # If it's the active file we loop back around as we might get some input
          # later
          then = datetime.datetime.now()
          currlogfile = glob.glob("%d%02d%02d-streams.log" % (then.year, then.month, then.day))
          if currlogfile != os.path.basename(filename):
            print >>sys.stderr, "Stop watching %s" % filename
            break

    proc1.terminate()

def enqueue_logs(queue):
    def launch_file_thread(queue, filename):
        t = Thread(target=enqueue_file, args=(queue, filename))
        t.daemon = True # thread dies with the program
        t.start()

    # Get initial log file name
    logfiles = glob.glob("/var/log/nginx/*-streams.log")
    print >>sys.stderr, "Found logs %s" % logfiles

    # Launch tails for any initial files
    for f in logfiles:
      launch_file_thread(queue,f)

    # Use inotify to watch for new files appearing
    wm = pyinotify.WatchManager()
    mask = pyinotify.IN_CREATE  # watched events

    class PLogs(pyinotify.ProcessEvent):
        def process_IN_CREATE(self, event):
            newfile = os.path.join(event.path, event.name)
            if newfile.endswith("-streams.log"):
                print >>sys.stderr, "Start watching: %s" %  newfile
                launch_file_thread(queue,newfile)

    notifier = pyinotify.ThreadedNotifier(wm, PLogs())
    wdd = wm.add_watch('/var/log/nginx/', mask, rec=True)

    notifier.start()




INTERVAL = 15
INTERVALMS = INTERVAL * 1000

def kill(proc):
    """Kills the subprocess given in argument."""
    # Clean up after ourselves.
    #proc.stdout.close()
    rv = proc.poll()
    if rv is None:

        rv = proc.poll()
        if rv is None:
            os.kill(proc.pid, 9)  # Bang bang!
            rv = proc.wait()  # This shouldn't block too long.
    print >>sys.stderr, "warning: proc exited %d" % rv
    return rv


def do_on_signal(signum, func, *args, **kwargs):
    """Calls func(*args, **kwargs) before exiting when receiving signum."""
    def signal_shutdown(signum, frame):
        print >>sys.stderr, "got signal %d, exiting" % signum
        func(*args, **kwargs)
        sys.exit(128 + signum)
    signal.signal(signum, signal_shutdown)


#dln.www.we7.com 2.97.76.192 - - [10/Jul/2014:15:04:43 +0100] "GET /stream/musicSrc/0223ea2a17294bbd91671e494db72146_aac64-PRCD-517542_0001_00001_LL_64k_full.m4a?streamunique=a215f073-cc32-472a-b43d-fc4c63ff45b2&e=1405001683&st=etUnU2Yb78_g7MZzdkVWTA HTTP/1.1" 206 37231 (bytes 0-1730541/1730542) "-" "AppleCoreMedia/1.0.0.11D257 (iPhone; U; CPU OS 7_1_2 like Mac OS X; en_us)" HIT
#dln.www.we7.com 2.219.210.196 - - [10/Jul/2014:15:04:44 +0100] "GET /stream/musicSrc/d054e6df6f3141d7a1665ef99fff7557_1116254170033.mp3?streamunique=a4484afa-81b9-4165-b23e-98ce07c7d05b&e=1405001680&st=BEzGpYB1eXyfEXlgK_g8mw HTTP/1.1" 200 5738369 (-) "-" "blinkbox%20music/3.5.1 CFNetwork/672.1.15 Darwin/14.0.0" HIT

def main(argv):
    # ignore SIGCHLD, prevent the zombie apocalypse
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)

    then = datetime.datetime.now()

    q = Queue()
    t = Thread(target=enqueue_logs, args=(q,))
    t.daemon = True # thread dies with the program
    t.start()

    oldtime = int(round(time.time() * 1000)) - INTERVALMS*2
    requests_hash = {}
    bytessent_hash = {}
    min_bytessent_hash = {}
    max_bytessent_hash = {}

    while True:
        line = q.get()

        if line.startswith("dl"):
            fields      = line.split(" ")
            http_host   = fields[0]
            http_method = fields[6].lstrip('"')
            url         = fields[7].split('?')
            http_path   = url[0]

            # Try and determine a file format for a music stream
            fformat = "none"
            if http_path.startswith("/stream/musicSrc/"):
                l = http_path.split('.')
                if len(l) >= 2:
                  fformat = l[len(l) - 1]

            http_status = fields[9]

            cache_state = fields[len(fields) - 1].rstrip('\n')
            if cache_state != "HIT" and cache_state != "MISS":
                cache_state = "UNKNOWN"

            bytessent = fields[10]
    
            tag = "http_host=%s http_status=%s cache_state=%s format=%s" % (
                  http_host,
                  http_status,
                  cache_state,
                  fformat,
                  )
            
            if tag in requests_hash:
                requests_hash[tag] += 1
            else:
                requests_hash[tag] = 1

            if bytessent.isdigit():
                val = int(bytessent)
                if tag in bytessent_hash:
                    bytessent_hash[tag] += val
                else:
                    bytessent_hash[tag] = val

                if tag in min_bytessent_hash:
                    min_bytessent_hash[tag] = min(min_bytessent_hash[tag], val) 
                else:
                    min_bytessent_hash[tag] = val

                if tag in max_bytessent_hash:
                    max_bytessent_hash[tag] = max(max_bytessent_hash[tag], val) 
                else:
                    max_bytessent_hash[tag] = val

        newtime = int(round(time.time() * 1000))
          
        if (newtime - oldtime) >= INTERVALMS:
            for tag in requests_hash.keys():
                sys.stdout.write("nginx.streams.requests %d %d %s\n" % (int(newtime / 1000) , requests_hash[tag] ,tag))
            for tag in bytessent_hash.keys():
                sys.stdout.write("nginx.streams.bytes_sent %d %d %s\n" % (int(newtime / 1000) , bytessent_hash[tag] ,tag))
            for tag in min_bytessent_hash.keys():
                sys.stdout.write("nginx.streams.bytes_sent_min %d %d %s\n" % (int(newtime / 1000) , min_bytessent_hash[tag] ,tag))
            for tag in max_bytessent_hash.keys():
                sys.stdout.write("nginx.streams.bytes_sent_max %d %d %s\n" % (int(newtime / 1000) , max_bytessent_hash[tag] ,tag))
            sys.stdout.flush()
            oldtime = newtime
            min_bytessent_hash = {}
            max_bytessent_hash = {}

    return 0  # Ask the tcollector to re-spawn us.

if __name__ == "__main__":
    sys.exit(main(sys.argv))


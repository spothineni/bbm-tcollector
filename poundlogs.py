#!/usr/bin/python
from collectors.lib import utils
import subprocess
from select import select
import signal
import os
import sys
import datetime
import time
import string
import urlparse
import glob
import select
from threading  import Thread
import os
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
          currlogfile = glob.glob("%d%02d%02d-%02d00pound.log" % (then.year, then.month, then.day, then.hour))
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
    logfiles = glob.glob('/var/log/pound/*/*/*-pound.log')
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
            if newfile.endswith("-pound.log"):
                print >>sys.stderr, "Start watching: %s" %  newfile
                launch_file_thread(queue,newfile)

    notifier = pyinotify.ThreadedNotifier(wm, PLogs())
    wdd = wm.add_watch('/var/log/pound/', mask, rec=True)

    notifier.start()

# Dec 11 15:14:15 hera pound: api.staginga.we7.com 192.168.90.1 - - [11/Dec/2013:15:14:15 +0000] "GET /api/0.1/user/anonymousUserId?apiKey=radiosite&appVersion=1&locale=en_GB HTTP/1.1" 200 47 "" "we7 HttpClient dredd-1386688578 (we7-no-session)" (- -> 10.0.3.14:8080) 0.012 sec

INTERVAL = 15
INTERVALMS = INTERVAL * 1000

def kill(proc):
    """Kills the subprocess given in argument."""
    # Clean up after ourselves.
    #proc.stdout.close()
    rv = proc.poll()
    if rv is None:
        os.kill(proc.pid, 15)
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


def main(argv):
    # ignore SIGCHLD, prevent the zombie apocalypse
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)

    then = datetime.datetime.now()

    q = Queue()
    t = Thread(target=enqueue_logs, args=(q,))
    t.daemon = True # thread dies with the program
    t.start()

    # Pretend like it is 2 intervals ago so that we print the
    # first summation
    oldtime = int(round(time.time() * 1000))

    requests_hash = {}
    bytessent_hash = {}
    duration_hash = {}
    min_duration_hash = {}
    max_duration_hash = {}

    while True:
        line = q.get()

        if line == QUEUE_FINISHED:
            break

        fields = line.split()
        if len(fields) > 20:
            loghost     = fields[3]

            http_host   = fields[5]
            # Some error lines match this parsing, could use a proper regex, but
            # we'll skip the stuff we know is crap, 'cos I'm lazy
            if http_host.startswith("("):
              continue

            http_method = fields[11].lstrip('"')
            url         = fields[12].split('?')
            http_path   = url[0]
            api_key     = ""
            if len(url) > 1:
                http_args   = urlparse.parse_qs(url[1])
                if "apiKey" in http_args:
                    api_key = http_args['apiKey'][0]
            
            http_port=False

            if ':' in http_host:
                host_port = string.split(http_host,':',1)
                http_host = host_port[0]
                http_port = host_port[1]
            else:
                http_port = "80"

            http_proto  = fields[13].rstrip('"')
            http_status = fields[14]
    
            duration    = fields[len(fields) - 2]
            browser_end = len(fields) - 5
            browser     = string.join(fields[17:browser_end]," ").replace('"',"").replace(" ","_")
    
            bytessent   = fields[15]
    
            tag = "http_host=%s http_status=%s http_method=%s" % (
                  http_host,
                  http_status,
                  http_method,
                  )
            
            # We need to indicate if we have no api key, or we get
            # overlapping duplicate tsdb series
            if api_key != "":
                tag = tag + " api_key=" + api_key
            else:
                tag = tag + " api_key=none"

            if http_port != False:
                tag = tag + " http_port=" + http_port

            if tag in requests_hash:
                requests_hash[tag] += 1
            else:
                requests_hash[tag] = 1

            if bytessent.isdigit():
                if tag in bytessent_hash:
                    bytessent_hash[tag] += int(bytessent)
                else:
                    bytessent_hash[tag] = int(bytessent)
          
            try: 
                val = float(duration)
                if tag in duration_hash:
                    duration_hash[tag] += val
                else:
                    duration_hash[tag] = val
                if tag in min_duration_hash:
                    min_duration_hash[tag] = min(min_duration_hash[tag], val) 
                else:
                    min_duration_hash[tag] = val
                if tag in max_duration_hash:
                    max_duration_hash[tag] = max(max_duration_hash[tag], val) 
                else:
                    max_duration_hash[tag] = val
            except ValueError:
                 True
          
        newtime = int(round(time.time() * 1000))

        if (newtime - oldtime) >= INTERVALMS:
            for tag in requests_hash.keys():
                sys.stdout.write("poundlogs.requests %d %d %s\n" % (int(newtime / 1000), requests_hash[tag] ,tag))
            for tag in bytessent_hash.keys():
                sys.stdout.write("poundlogs.bytes_sent %d %d %s\n" % (int(newtime / 1000) , bytessent_hash[tag] ,tag))
            for tag in duration_hash.keys():
                sys.stdout.write("poundlogs.duration %d %f %s\n" % (int(newtime / 1000) , duration_hash[tag] ,tag))
            for tag in min_duration_hash.keys():
                sys.stdout.write("poundlogs.duration_min %d %f %s\n" % (int(newtime / 1000) , min_duration_hash[tag] ,tag))
            for tag in max_duration_hash.keys():
                sys.stdout.write("poundlogs.duration_max %d %f %s\n" % (int(newtime / 1000) , max_duration_hash[tag] ,tag))
            sys.stdout.flush()
            oldtime = newtime
            min_duration_hash = {}
            max_duration_hash = {}

    return 0  # Ask the tcollector to re-spawn us.

if __name__ == "__main__":
    sys.exit(main(sys.argv))


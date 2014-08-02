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
    logfiles = glob.glob("/var/log/java/*/*/*-streams.log")
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
            if newfile.endswith("-stream.log"):
                print >>sys.stderr, "Start watching: %s" %  newfile
                launch_file_thread(queue,newfile)

    notifier = pyinotify.ThreadedNotifier(wm, PLogs())
    wdd = wm.add_watch('/var/log/java/', mask, rec=True)

    notifier.start()



#Dec 12 23:47:16 10.0.3.13 StreamInfo:STREAMID=1965267527|KEY=samsung|SESSION=60CE162EBC34A0675B8912CD36170C50|IP=86.143.165.23|TRACK=5668994|ADFREE=false|ADVERTID=-1|USERID=154781|STREAMUNIQUE=83f39b91-bc1a-42a9-a813-bf46bbdc6d3b|COUNTRY=GB|GENRE=15|DATE=20131212-23:47:16.555|SOURCE=RADIO|FLAGS=0|ARTIST=121970|ALBUM=587978|MANAGER=186481|TRACKVERSION=2315254
#Dec 12 23:51:15 10.0.3.11 StreamEnd:STREAMUNIQUE=83f39b91-bc1a-42a9-a813-bf46bbdc6d3b|PLAYEDTIME=238.154|PAUSEDTIME=0

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


def main(argv):
    # ignore SIGCHLD, prevent the zombie apocalypse
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)

    then = datetime.datetime.now()

    q = Queue()
    t = Thread(target=enqueue_logs, args=(q,))
    t.daemon = True # thread dies with the program
    t.start()

    oldtime = int(round(time.time() * 1000)) - INTERVALMS*2

    starts_hash = {}

    ends_hash = {}
    ends_hash['played'] = 0
    ends_hash['paused'] = 0

    while True:
        line = q.get()

        fields = line.split()
        if len(fields) == 5:
            logdata = fields[4].split(':',1)
            call = ""
            data = ""
            if len(logdata) == 1:
              call = "StreamInfo"
              data = logdata[0]
            elif len(logdata) == 2:
              call = logdata[0]
              data = logdata[1]
            else:
              continue

            records = {}
            try:
                for r in data.split('|'):
                     k,v = r.split('=',1)
                     records[k] = v
            except:
                continue

            if call.startswith('StreamInfo'):
                tag = "key=%s country=%s source=%s" % (
                    records['KEY'],
                    records['COUNTRY'],
                    records['SOURCE'])
                if tag in starts_hash:
                    starts_hash[tag] += 1
                else:
                    starts_hash[tag] = 1
            elif call.startswith('StreamEnd'):
                #ends_hash['played'] += int(float(records['PLAYEDTIME']) * 1000.0)
                #ends_hash['paused'] += int(float(records['PAUSEDTIME']) * 1000.0)
                if 'PLAYEDTIME' in records:
                    ends_hash['played'] += int(records['PLAYEDTIME'])
                if 'PAUSEDTIME' in records:
                    ends_hash['paused'] += int(records['PAUSEDTIME'])
            else:
                continue

        newtime = int(round(time.time() * 1000))

        if (newtime - oldtime) >= INTERVALMS:
            for tag in starts_hash.keys():
                sys.stdout.write("streams.requests %d %d %s\n" % (int(newtime / 1000), starts_hash[tag] ,tag))

            for tag in ends_hash.keys():
                sys.stdout.write("streams.duration %d %d type=%s\n" % (int(newtime / 1000) , ends_hash[tag] ,tag))

            sys.stdout.flush()
            oldtime = newtime

        now = datetime.datetime.now()

    return 0  # Ask the tcollector to re-spawn us.

if __name__ == "__main__":
    sys.exit(main(sys.argv))


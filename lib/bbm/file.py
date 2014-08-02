from bbm import TSDBMetricData
from bbm.process import enqueue_process
import re
import os
import sys
import select
from threading  import Thread
import pyinotify
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # python 3.x

def tail_file_to_queue(queue, filename, currFileDateStr, mapFunc=None):
    # Get initial log file name
    def onTimeOut():
        then = datetime.datetime.now()
        currlogfile = glob.glob(currFileDateStr % (then.year, then.month, then.day, then.hour))
        if currlogfile != os.path.basename(filename):
            print >>sys.stderr, "Stop watching %s" % filename
            return False
        else:
            return True

    command = 'tail'
    args = ["--lines", "0", '-F'] + [ filename ]
    enqueue_process(
        queue,
        command,
        args,
        onTimeOut,
        mapFunc)

def enqueue_files(queue, logDir, initFileGlob, currFileDateStr, mapFunc=None):

    def launch_file_thread(queue, filename):
        t = Thread(target=enqueue_file, args=(queue, filename, currFileDateStr, mapFunc))
        t.daemon = True # thread dies with the program
        t.start()

    # Get initial log file name
    logfiles = glob.glob(logDir + "/" + initFileGlob)
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
    wdd = wm.add_watch(logDir, mask, rec=True)

    notifier.start()

def start_files_collector(logDir, initFileGlob, currFileDateStr, mapFunc=None):
    q = Queue()

    t = Thread(target=enqueue_files, args=(logDir, initFileGlob , currFileDateStr, mapFunc))
    t.daemon = True # thread dies with the program
    t.start()

    return q

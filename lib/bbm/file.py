from bbm.process import enqueue_process
import os
import glob
import sys
import datetime
import fnmatch
import pyinotify
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # python 3.x

def defaultOnFileTimeOut(f):
    return True

# Tail the current contents of a file into a python queue
def enqueue_file(queue, filename, onFileTimeOut=defaultOnFileTimeOut, mapFunc=None):
    def onFileTimeOutThunk():
        onFileTimeOut(filename)

    command = 'tail'
    args = ["--lines", "0", '-F'] + [ filename ]
    enqueue_process(
        queue,
        command,
        args,
        onTimeOut=onFileTimeOutThunk,
        mapFunc=mapFunc)

# Enqueue all files under base, matching apttern. Use inotify to add any new files that
# appear
def enqueue_files(queue, base, pattern, fileFilter=None, onFileTimeOut=defaultOnFileTimeOut, mapFunc=None):
    def defaultFileFilter(f):
        print "calling default"
        return True

    isWanted = defaultFileFilter
    if fileFilter != None:
        isWanted = fileFilter

    def launch_file_thread(queue, filename):
        t = Thread(target=enqueue_file, args=(queue, filename, onFileTimeOut, mapFunc))
        t.daemon = True # thread dies with the program
        t.start()

    # Get initial file names
    files = glob.glob(base + "/" + pattern)

    # Launch tails for any initial files
    for f in files:
        if isWanted(f) == True: 
            print >>sys.stderr, "Start watching initial file %s" % f
            launch_file_thread(queue,f)

    # Use inotify to watch for new files appearing
    wm = pyinotify.WatchManager()
    mask = pyinotify.IN_CREATE  # watched events

    class PLogs(pyinotify.ProcessEvent):
        def process_IN_CREATE(self, event):
            newfile = os.path.join(event.path, event.name)
            if isWanted(f) == True and fnmatch.fnmatch(newfile, base + "/" + pattern):
                print >>sys.stderr, "Start watching: %s" % newfile
                launch_file_thread(queue,newfile)
            else:
                print >>sys.stderr, "Ignoring: %s" % newfile
                

    wdd = wm.add_watch(base, mask, rec=True)
    notifier = pyinotify.ThreadedNotifier(wm, PLogs())

    notifier.start()

# Enqueue all files matching pattern. use dateStr to identify the file
# matching the current log file, to allow us to stop watching old log files
# when log content is no longer being written to them,
def enqueue_dated_files(queue, base, pattern, dateStr, mapFunc=None):
    def isCurrentLog(filename):
        now = datetime.datetime.now()
        # Determine what log file is current for the time right now
        return filename == base + "/" + now.strftime(dateStr)

    enqueue_files(queue, base, pattern, fileFilter=isCurrentLog, onFileTimeOut=isCurrentLog, mapFunc=mapFunc)

def start_dated_files_collector(logDir, initFileGlob, currFileDateStr, mapFunc=None):
    q = Queue()

    t = Thread(target=enqueue_dated_files, args=(q, logDir, initFileGlob , currFileDateStr, mapFunc))
    t.daemon = True # thread dies with the program
    t.start()

    return q

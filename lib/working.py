import os
import re
import subprocess
import sys
import time
import select
from threading  import Thread
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # python 3.x
import pyinotify


QUEUE_FINISHED = object()
ON_POSIX = 'posix' in sys.builtin_module_names
POLL_TIMEOUT_MS = 300000 # 5 minutes

#currlogfile = glob.glob("%d%02d%02d-%02d00pound.log" % (then.year, then.month, then.day, then.hour))

class TSDBMetricData:
    def __init__(self, metric, value, tags=[]):
        self.metric = metric
        self.value = value
        self.tags = tags
    def __str__(self):
        return "<metric: %s, value: %s, tags: %s>" % (self.metric, self.value, self.tags)

def onTimeOutTrue():
    return True

def enqueue_process(queue, cmd, args, timeout=POLL_TIMEOUT_MS, onTimeOut=onTimeOutTrue,mapFunc=None):
    # Get initial log file name
    command = [ cmd ] + args

    try:
        proc1 = subprocess.Popen(command, stdout=subprocess.PIPE, bufsize=0, close_fds=ON_POSIX)

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
    args = ["--lines", "0", '-f'] + [ filename ]
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

class JMXPattern:
    def __init__(self, query, attrRegexStr, handleFunc=None):
        self.query = query
        self.queryRegex = re.compile(".*" + query + ".*")
        self.attrRegexStr = attrRegexStr
        self.attrRegex = re.compile(attrRegexStr)
        self.handleFunc = handleFunc


def enqueue_jmx(queue, interval, pid, patterns=[], mapFunc=None):

    def jmx2metric(jmxData):
        metric = "jmx."
        metric += jmxData["bean"]["name"] + "."
        metric += jmxData["attribute"]
        value = jmxData["value"]
        tags = map(lambda x : x["key"] + "=" + x["value"], jmxData["bean"]["attrs"])
        return TSDBMetricData(metric, value, tags)

    def splitJMXLine(line):
        def splitKV(s):
            d = s.split('=',1)
            return { "key": d[0], "value": d[1] }

        data = line.split('\t')
        time = data[0]
        attribute = data[1]
        value = data[2]
        beanData = data[3]
        beanName, beanAttrStr = beanData.split(':',1)
        beanAttrs = map(splitKV, beanAttrStr.split(','))
        result = {
                     "time":      time,
                     "attribute": attribute,
                     "value":     value,
                     "bean": { "name": beanName, 
                              "attrs": beanAttrs }
                 }
        m = jmx2metric(result)

        for p in patterns:
            if p.handleFunc != None and p.queryRegex.match(beanData) and p.attrRegex.match(attribute):
                m = p.handleFunc(m)
                break
          
        if mapFunc == None:
            return m
        else:
            return mapFunc(m)

    JAVA_HOME = os.getenv("JAVA_HOME")

    if JAVA_HOME == None:
        JAVA_HOME = "/usr/lib/jvm/default-java"

    JAVA = "%s/bin/java" % JAVA_HOME

    # We add those files to the classpath if they exist.
    CLASSPATH = [
        "%s/lib/tools.jar" % JAVA_HOME,
    ]
    # Build the classpath.
    dir = os.path.dirname(sys.argv[0])
    jar = os.path.normpath(dir + "/../lib/jmx-1.0.jar")
    if not os.path.exists(jar):
        print >>sys.stderr, "WTF?!  Can't run, %s doesn't exist" % jar
        return 13
    classpath = [jar]
    for jar in CLASSPATH:
        if os.path.exists(jar):
            classpath.append(jar)
    classpath = ":".join(classpath)

    pStrArray = []
    for p in patterns:
        if not isinstance(p, JMXPattern):
            raise "pattern must be an instance of JMXPatter"
        else:
            pStrArray = pStrArray + [p.query , p.attrRegexStr]

    enqueue_process(
        queue,
        JAVA,
        [ "-enableassertions", "-enablesystemassertions",  # safe++
          "-Xmx64m",
          "-cp", classpath, "com.stumbleupon.monitoring.jmx",
          "--watch", str(interval) , "--long", "--timestamp",
          pid ] + pStrArray,
        mapFunc=splitJMXLine)

def start_jmx_collector(interval, pid, patterns=[], mapFunc=None):
    q = Queue()

    t = Thread(target=enqueue_jmx, args=(q, interval, pid , patterns, mapFunc))
    t.daemon = True # thread dies with the program
    t.start()

    return q

def start_jvm_collector(interval, pid, patterns=[], mapFunc=None):
    def threading2tsdb(v):
        metric = "jvm.threads."
        attr = v.metric[14:-11].lower()
        if attr == "":
            attr = "current"
        elif attr == "totalstarted":
            attr = "total"
        metric = metric + attr
        value = v.value
        return TSDBMetricData(metric,value)
    def memory2tsdb(v):
        metric = "jvm.memory."
        # e.g. jmx.java.lang.HeapMemoryUsage
        memtype = v.metric[14:-11].lower()
        tags = [ "type=" + memtype ]
        newval =  TSDBMetricData(metric,"0", tags)

        # e.g. big horrid string thing
        valuesStr = v.value.split("{")[1].split("}")[0]
        values = map(lambda x: x.strip(" "), valuesStr.split(","))
        results = []
        for value in values:
          valtype, valval = value.split("=")
          results = results + [ TSDBMetricData(metric + valtype, valval, tags) ]
        return results
    return start_jmx_collector(
        interval, 
        pid , 
        [
            JMXPattern("java.lang:type=Threading","^(PeakThread|DaemonThread|Thread|TotalStartedThread)Count$",threading2tsdb),
            JMXPattern("java.lang:type=Memory", "^(NonHeap|Heap)MemoryUsage$",memory2tsdb),
        ] + patterns,
        mapFunc)

def start_tomcat_collector(interval, pid, patterns=[], mapFunc=None):
    def accesses2tsdb(v):
        metric = "tomcat.accesses"
        webappName = v.metric[4:-12]
        value = v.value
        for t in v.tags:
            if t.startswith("name="):
                tags = ["webapp=" + webappName, "service=" + t[5:]]
                break
        return TSDBMetricData(metric,value,tags)
    return start_jvm_collector(
        interval, 
        pid , 
        [JMXPattern("type=GlobalRequestProcessor", "^requestCount$", accesses2tsdb)] + patterns,
        mapFunc)

def start_c3p0_collector(interval, pid, patterns=[], mapFunc=None):
    def c3p0conns2tsdb(v):
        metric = "c3p0.connections."
        attr = v.metric[27:-11].lower()
        if attr == "":
            attr = "total"
        metric = metric + attr
        value = v.value
        return TSDBMetricData(metric,value)
        return metric
    return start_jvm_collector(
        interval, 
        pid , 
        [JMXPattern("com.mchange.v2.c3p0:type=PooledDataSource", "^num.*Connections$", c3p0conns2tsdb)] + patterns,
        mapFunc)


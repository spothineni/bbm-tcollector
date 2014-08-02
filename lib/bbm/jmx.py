from bbm import TSDBMetricData
from bbm.process import enqueue_process
import re
import os
import sys
import select
from threading  import Thread
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # python 3.x

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
    jar = "/usr/lib/tcollector/bbm/lib/jmx-1.0.jar"
    if not os.path.exists(jar):
        print >>sys.stderr, "Can't run, %s doesn't exist" % jar
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


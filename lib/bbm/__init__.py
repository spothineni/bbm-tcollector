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

ValidTSDBStringRegex = re.compile("[a-zA-Z0-9_/\-\.]+")
class TSDBMetricData:
    def __init__(self, metric, value, tags=[]):
        self.metric = metric
        self.value = value
        self.tags = tags
    def __str__(self):
        return "<metric: %s, value: %s, tags: %s>" % (self.metric, self.value, self.tags)
    def isValid(self):
        if not ValidTSDBStringRegex.match(self.metric):
            return False, "Invalid metric name: %s" % self.metric

        for t in self.tags:
            tagk, tagv = t.split("=",1)
            if not ValidTSDBStringRegex.match(tagk):
                return False, "Invalid tag key: %s" % tagk
            if not ValidTSDBStringRegex.match(tagv):
                return False, "Invalid tag value: %s" % tagv

        try:
            float(self.value)
        except ValueError:
            return False, "Value cannot be converted to numeric: %s" % v.value

        return True, "OK"

    def output(self, t):
        valid, err = self.isValid()      
        if valid:
            print >>sys.stdout, "%s %d %s %s" % (self.metric, t, self.value, " ".join(self.tags))
        else:
            print >>sys.stderr, "Refusing to output %s, %s" % (self, err)

def RunCollector(q):
    while True:
        t = time.time()
        line = q.get()
        if line != QUEUE_FINISHED:
            line.output(t)
        else:
            break

#!/usr/bin/python

import signal
import sys
import subprocess
from collectors.lib  import utils
from bbm import RunCollector
from bbm.jmx import start_jmx_collector
from bbm.jmx import JMXPattern

# These are the jmx handlers we'll be using.
from bbm.jvm import jvm_collector

signal.signal(signal.SIGCHLD, signal.SIG_IGN)

# Find the pid of the bbm-core-api server
pgrep = subprocess.check_output(["/usr/bin/pgrep","-u", "voldemort", "-f", "voldemort.server.VoldemortServer"])
jpid = pgrep.rstrip("\n")
if jpid == "":
   sys.exit(1)

# We can change over to hte bbm-core-api user for secturity
utils.drop_privileges(user="voldemort")

def rewriter(v):
    # Strip off leading
    if v.metric.startswith("jmx.voldemort.store.stats."):
        v.metric = v.metric[len("jmx.voldemort.store.stats."):]
        if v.metric.startswith("aggregate"):
            return []
        v.tags = [ "store=" + x[len("type="):] for x in  v.tags if x.startswith("type=") ]
        if v.metric.startswith("numberOfCallsTo"):
            op = v.metric[len("numberOfCallsTo"):].lower()
            v.metric = "voldemort.calls"
            v.tags = v.tags + ["op=" + op]
            return v
        if v.metric.endswith("CompletionTimeInMs"):
            op = v.metric[len("average"):-1 * len("CompletionTimeInMs")].lower()
            v.metric = "voldemort.avgTime"
            v.tags = v.tags + ["op=" + op]
            return v
        if v.metric.endswith("averageOperationTimeInMs"):
            op = "all"
            v.metric = "voldemort.avgTime"
            v.tags = v.tags + ["op=" + op]
            return v
    return v

voldemort_collector = [JMXPattern("voldemort.store.stats", 'numberOfCalls|InMs$')]

RunCollector(start_jmx_collector(15, jpid, jvm_collector + voldemort_collector, rewriter), extraTags=["application=voldemort"])


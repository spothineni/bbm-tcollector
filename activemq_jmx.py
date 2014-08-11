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
pgrep = subprocess.check_output(["/usr/bin/pgrep","-u", "activemq", "-f", "xbean:activemq.xml"])
jpid = pgrep.rstrip("\n")
if jpid == "":
   sys.exit(1)

# We can change over to hte bbm-core-api user for secturity
utils.drop_privileges(user="activemq")

def rewriter(v):
    if v.metric.startswith("jmx.org.apache.activemq."):
        # Strip off leading
        v.metric = v.metric[len("jmx.org.apache.activemq."):]
        metrictype = None
        for t in v.tags:
            if t.startswith("Type="):
                metrictype = t[len("Type="):].lower()
                break

        if metrictype == None:
            return []

        # The only tag we'll keep is the queue destination
        v.tags = [t.lower() for t in v.tags if t.startswith("Destination=")]

        if v.metric.endswith("Count"):
            v.metric = v.metric[:-1 * len("Count")].lower() + "_count"
            if v.metric.startswith("total"):
                v.metric = v.metric[len("total"):]

        if v.metric.endswith("Limit"):
            v.metric = v.metric[:-1 * len("Limit")].lower() + "_limit"

        if v.metric.endswith("PercentUsage"):
            v.metric = v.metric[:-1 * len("PercentUsage")].lower() + "_percent_usage"

        if v.metric.endswith("QueueSize"):
            v.metric = v.metric[:-1 * len("QueueSize")] + "size"

        if v.metric == "MemoryUsagePortion":
            v.metric = "memory_portion"

        if v.metric == "CursorMemoryUsage":
            v.metric = "cursor_memory_usage"

        if v.metric == "MaxPageSize":
            v.metric = "page_size.max"

        if v.metric.endswith("Time"):
            m = v.metric[:-1 * len("time")]
            if m.startswith("Min"):
                m = m[len("Min"):].lower() + "_time.min"
            if m.startswith("Max"):
                m = m[len("Max"):].lower() + "_time.max"
            if m.startswith("Average"):
                m = m[len("Average"):].lower() + "_time.average"
            v.metric = m

        v.metric = "activemq.%s.%s" % (metrictype, v.metric)

    return v

activemq_collector = [JMXPattern("Type=Broker", "Count$|Usage$|Limit$"), # Broker specific stats
                       JMXPattern(",Type=Queue", "Count$|Usage$|Limit$|Portion$|Time$|Size$")] # Queue specific stats

RunCollector(start_jmx_collector(15, jpid, jvm_collector + activemq_collector, rewriter), extraTags=["application=activemq"])


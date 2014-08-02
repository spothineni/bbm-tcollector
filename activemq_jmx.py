#!/usr/bin/python
# This file is part of tcollector.
# Copyright (C) 2010  The tcollector Authors.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.  This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
# General Public License for more details.  You should have received a copy
# of the GNU Lesser General Public License along with this program.  If not,
# see <http://www.gnu.org/licenses/>.

# These are the beans we want to monitor
#1405078198	TotalEnqueueCount	89873	org.apache.activemq:BrokerName=cottus,Type=Broker
#1405078198	TotalDequeueCount	273587	org.apache.activemq:BrokerName=cottus,Type=Broker
#1405078198	TotalConsumerCount	1459	org.apache.activemq:BrokerName=cottus,Type=Broker
#1405078198	TotalProducerCount	0	org.apache.activemq:BrokerName=cottus,Type=Broker
#1405078198	TotalMessageCount	32	org.apache.activemq:BrokerName=cottus,Type=Broker
#1405078198	MemoryPercentUsage	0	org.apache.activemq:BrokerName=cottus,Type=Broker
#1405078198	MemoryLimit	67108864	org.apache.activemq:BrokerName=cottus,Type=Broker
#1405078198	StoreLimit	104857600000	org.apache.activemq:BrokerName=cottus,Type=Broker
#1405078198	StorePercentUsage	0	org.apache.activemq:BrokerName=cottus,Type=Broker
#1405078198	TempLimit	52428800000	org.apache.activemq:BrokerName=cottus,Type=Broker
#1405078198	TempPercentUsage	0	org.apache.activemq:BrokerName=cottus,Type=Broker
#
#1405078432	ProducerCount	0	org.apache.activemq:BrokerName=cottus,Type=Queue,Destination=/ircalerts
#1405078432	ConsumerCount	0	org.apache.activemq:BrokerName=cottus,Type=Queue,Destination=/ircalerts
#1405078432	EnqueueCount	0	org.apache.activemq:BrokerName=cottus,Type=Queue,Destination=/ircalerts
#1405078432	DequeueCount	0	org.apache.activemq:BrokerName=cottus,Type=Queue,Destination=/ircalerts
#1405078432	DispatchCount	0	org.apache.activemq:BrokerName=cottus,Type=Queue,Destination=/ircalerts
#1405078432	InFlightCount	0	org.apache.activemq:BrokerName=cottus,Type=Queue,Destination=/ircalerts
#1405078432	ExpiredCount	0	org.apache.activemq:BrokerName=cottus,Type=Queue,Destination=/ircalerts
#1405078432	AverageEnqueueTime	0.0	org.apache.activemq:BrokerName=cottus,Type=Queue,Destination=/ircalerts
#1405078432	MaxEnqueueTime	0	org.apache.activemq:BrokerName=cottus,Type=Queue,Destination=/ircalerts
#1405078432	MinEnqueueTime	0	org.apache.activemq:BrokerName=cottus,Type=Queue,Destination=/ircalerts
#1405078432	CursorMemoryUsage	0	org.apache.activemq:BrokerName=cottus,Type=Queue,Destination=/ircalerts
#1405078432	CursorPercentUsage	0	org.apache.activemq:BrokerName=cottus,Type=Queue,Destination=/ircalerts
#1405078432	MemoryLimit	1048576	org.apache.activemq:BrokerName=cottus,Type=Queue,Destination=/ircalerts


#1405078432	QueueSize	0	org.apache.activemq:BrokerName=cottus,Type=Queue,Destination=/ircalerts


#1405078432	MemoryPercentUsage	0	org.apache.activemq:BrokerName=cottus,Type=Queue,Destination=/ircalerts
#1405078432	MemoryUsagePortion	0.0	org.apache.activemq:BrokerName=cottus,Type=Queue,Destination=/ircalerts

import os
import re
import signal
import subprocess
import sys
import time
from collectors.lib import utils
from threading import Thread

INTERVAL=15

# If this user doesn't exist, we'll exit immediately.
# If we're running as root, we'll drop privileges using this user.
USER = "Debian-storm"

# Use JAVA_HOME env variable if set
JAVA_HOME = os.getenv('JAVA_HOME', '/usr/lib/jvm/java-1.6.0-openjdk-1.6.0.0.x86_64')
JAVA = "%s/bin/java" % JAVA_HOME

# We add those files to the classpath if they exist.
CLASSPATH = [
    "%s/lib/tools.jar" % JAVA_HOME,
]

# We shorten certain strings to avoid excessively long metric names.
JMX_SERVICE_RENAMING = {
    "GarbageCollector": "jvm.gc",
    "OperatingSystem": "jvm.os",
    "Threading": "jvm.threads",
}

EXCLUDE_QUEUES=re.compile("^mcollective\.reply\..*")

def main(argv):
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)
    pgrep = subprocess.check_output(["/usr/bin/pgrep","-u", "activemq", "java"])
    jpid = pgrep.rstrip("\n") 

    # This should have bailed if no pid was found as pgrep returns 1!
    if jpid == "":
      return 1

    utils.drop_privileges(user=USER)

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

    print >>sys.stderr, "Processing %s " % jpid
    cmd = [JAVA,  "-enableassertions", "-enablesystemassertions",  # safe++
         "-Xmx64m",  # Low RAM limit, to avoid stealing too much from prod.
         "-cp", classpath, "com.stumbleupon.monitoring.jmx",
         "--watch", str(INTERVAL), "--long", "--timestamp",
         jpid,  # Name of the process.
         # The remaining arguments are pairs (mbean_regexp, attr_regexp).
         # The first regexp is used to match one or more MBeans, the 2nd
         # to match one or more attributes of the MBeans matched.
         "Threading", "Count$|Time$",       # Number of threads and CPU time.
         "OperatingSystem", "OpenFile",    # Number of open files.
         "GarbageCollector", "Collection", # GC runs and time spent GCing.
         "Type=Broker", "Count$|Usage$|Limit$", # Broker specific stats
         # leading comman prevents matching other beans
         ",Type=Queue", "Count$|Usage$|Limit$|Portion$|Time$|Size$" # Queue specific stats 
         ] 
    print >>sys.stderr, " ".join(cmd)

    jmx = subprocess.Popen(cmd , stdout=subprocess.PIPE)

    prev_timestamp = 0
    for line in  jmx.stdout:
        try:
            timestamp, metric, value, mbean = line.split("\t", 3)
        except ValueError, e:
            # Temporary workaround for jmx.jar not printing these lines we
            # don't care about anyway properly.
            if "java.lang.String" not in line:
                print >>sys.stderr, "Can't split line: %r" % line
            continue

        # Sanitize the timestamp.
        try:
            timestamp = int(timestamp)
            if timestamp < time.time() - 600:
                raise ValueError("timestamp too old: %d" % timestamp)
            if timestamp < prev_timestamp:
                raise ValueError("timestamp out of order: prev=%d, new=%d"
                                 % (prev_timestamp, timestamp))
        except ValueError, e:
            print >>sys.stderr, ("Invalid timestamp on line: %r -- %s"
                                 % (line, e))
            continue

        prev_timestamp = timestamp


        tags = ""
        jmx_service = ""

        # mbean is of the form "domain:key=value,...,foo=bar"
        mbean_domain, mbean_properties = mbean.rstrip().replace(" ", "_").split(":", 1)

        if mbean_domain == "java.lang":
            mbean_properties = dict(prop.split("=", 1)
                                for prop in mbean_properties.split(","))
            jmx_service = mbean_properties.pop("type", "jvm")
            if mbean_properties:
                tags += " " + " ".join(k + "=" + v for k, v in
                                       mbean_properties.iteritems())

            jmx_service = JMX_SERVICE_RENAMING.get(jmx_service, jmx_service)
            jmx_service, repl_count = re.subn("[^a-zA-Z0-9]+", ".",
                                              jmx_service)
            metric = jmx_service.lower() + "." + metric

        if mbean_domain == "org.apache.activemq":
            mbean_properties = dict(prop.split("=", 1)
                                for prop in mbean_properties.split(","))
            if mbean_properties['Type'] == "Broker":
                if metric.startswith("Total") and metric.endswith("Count"):
                    name = metric[5:-5].lower()
                    metric = "broker." + name +  "_count"
                elif metric.endswith("Limit"):
                    name = metric[:-5].lower()
                    metric = "broker." + name +  "_limit"
                elif metric.endswith("PercentUsage"):
                    name = metric[:-12].lower()
                    metric = "broker." + name +  "_percent_usage"
            elif mbean_properties['Type'] == "Queue":
                if EXCLUDE_QUEUES.match(mbean_properties['Destination']):
                    continue
                else:
                    tags += "queue=" + mbean_properties['Destination']

                    if metric.endswith("Count"):
                        name = metric[:-5].lower()
                        metric = "queue." + name +  "_count"
                    elif metric.endswith("EnqueueTime"):
                        name = metric[:-11].lower()
                        metric = "queue.enqueue_time." + name 
                    elif metric.startswith("Cursor") and metric.endswith("Usage"):
                        name = metric[6:-5].lower()
                        metric = "queue.cursor." + name + "_usage"
                    elif metric == "QueueSize":
                        metric = "queue.size"
                    elif metric.startswith("Memory"):
                        name = metric[6:].lower()
                        if name == "usageportion":
                          name = "portion_of_total"
                        elif name  == "percentusage":
                          name = "percent_usage"
                        metric = "queue.memory." + name
                    else:
                      continue
            else:
              continue


        sys.stdout.write("activemq.%s %d %s %s\n" % (metric, timestamp, value, tags))
        sys.stdout.flush()

    return 0  # Ask the tcollector to re-spawn us.


if __name__ == "__main__":
    sys.exit(main(sys.argv))


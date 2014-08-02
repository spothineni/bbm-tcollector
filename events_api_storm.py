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

import os
import re
import signal
import subprocess
import sys
import time
from collectors.lib import utils
from threading import Thread

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
    "userevents": "topology.events_api"
}


def kill(proc):
  """Kills the subprocess given in argument."""
  # Clean up after ourselves.
  proc.stdout.close()
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


def processJMX(jpid, topologyName, taskId, classpath):
    latencyPattern = re.compile("Latency$")
    statsPattern = re.compile("StatsDetail$")
    excludepattern = re.compile("Snapshot|__acker")
    procTaskPattern = re.compile('\.[0-9]+\.[0-9]+\.')
    domainStr = '.we7.local'

    print >>sys.stderr, "Processing %s " % jpid
    jmx = subprocess.Popen(
        [JAVA, "-enableassertions", "-enablesystemassertions",  # safe++
         "-Xmx64m",  # Low RAM limit, to avoid stealing too much from prod.
         "-cp", classpath, "com.stumbleupon.monitoring.jmx",
         "--long", "--timestamp",
         jpid,  # Name of the process.
         # The remaining arguments are pairs (mbean_regexp, attr_regexp).
         # The first regexp is used to match one or more MBeans, the 2nd
         # to match one or more attributes of the MBeans matched.
         "Threading", "Count$|Time$",       # Number of threads and CPU time.
         "OperatingSystem", "OpenFile",    # Number of open files.
         "GarbageCollector", "Collection", # GC runs and time spent GCing.
         "userevents", "Count$|CountTotal$|Latency$|StatsDetail$",
         ], stdout=subprocess.PIPE, bufsize=1)

    try:
        prev_timestamp = 0
        for line in jmx.stdout:
            if len(line) < 4:
                print >>sys.stderr, "invalid line (too short): %r" % line
                continue

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

            jmx_service = ""
            tags = "topology=" + topologyName + " task=" + taskId

            # mbean is of the form "domain:key=value,...,foo=bar"
            mbean_domain, mbean_properties = mbean.rstrip().replace(" ", "_").split(":", 1)
            if mbean_domain == "java.lang":
                mbean_properties = dict(prop.split("=", 1)
                                    for prop in mbean_properties.split(","))
                jmx_service = mbean_properties.pop("type", "jvm")
                if mbean_properties:
                    tags += " " + " ".join(k + "=" + v for k, v in
                                           mbean_properties.iteritems())
            elif mbean_domain == "userevents":
                jmx_service = mbean_domain
            else:
                print >>sys.stderr, ("Unexpected mbean domain = %r on line %r"
                                     % (mbean_domain, line))
                continue

            jmx_service = JMX_SERVICE_RENAMING.get(jmx_service, jmx_service)
            jmx_service, repl_count = re.subn("[^a-zA-Z0-9]+", ".",
                                              jmx_service)

            metric = jmx_service.lower() + "." + metric
            if excludepattern.search(metric):
                continue
            elif latencyPattern.search(metric):
                latencyList, repl_count = re.subn("[{},]+", "", value)
                if latencyList == "{}" or latencyList == '': continue

                latencyValues = latencyList.split(" ")

                for latency in latencyValues:
                    fqdnPos = latency.index(domainStr)
                    fqdn = latency[:fqdnPos + len(domainStr)]
                    procTask = procTaskPattern.match(latency[len(fqdn):])
                    if procTask == None: continue
                    taskDetail = latency[len(fqdn) + procTask.end():].split("=")

                    if excludepattern.search(taskDetail[0]):
                       continue

                    tags = "topology=" + topologyName + " task=" + taskId
                    tags += " metricHost=" + fqdn + " component=" + taskDetail[0]
                    value = taskDetail[1]

                    sys.stdout.write("storm.%s %d %s %s\n"
                          % (metric, timestamp, value, tags))
                    sys.stdout.flush()
            elif statsPattern.search(metric):
                statsList, repl_count = re.subn("[{},]+", "", value)
                if statsList == "{}" or statsList == '': continue
                statsValues = statsList.split(" ")

                for stats in statsValues:
                    fqdnPos = stats.index(domainStr)
                    fqdn = stats[:fqdnPos + len(domainStr)]
                    procTask = procTaskPattern.match(stats[len(fqdn):])
                    if procTask == None: continue
                    taskDetail = stats[len(fqdn) + procTask.end():].split(".")
                    metricDetail = taskDetail[1].split("=")

                    if excludepattern.search(metricDetail[0]):
                       continue

                    taskName, repl_count = re.subn("[^a-zA-Z0-9]+", "", taskDetail[0])
                    metricName, repl_count = re.subn("[_]+", "", metricDetail[0])
                    tags = "topology=" + topologyName + " task=" + taskId
                    tags += " metricHost=" + fqdn + " component=" + taskName
                    metric = "topology.events.api." + metricName
                    value = metricDetail[1]

                    sys.stdout.write("storm.%s %d %s %s\n"
                          % (metric, timestamp, value, tags))
                    sys.stdout.flush()
            else:
                sys.stdout.write("storm.%s %d %s %s\n"
                                 % (metric, timestamp, value, tags))
                sys.stdout.flush()
    finally:
        kill(jmx)

def main(argv):
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

    jpid = "worker"
    jps = subprocess.check_output("/usr/bin/jps").split("\n")
    for item in jps:
      vals = item.split(" ")
      if len(vals) == 2:
        if vals[1] == "worker":
          jmx = subprocess.Popen(
             [JAVA, "-enableassertions", "-enablesystemassertions",
              "-Xmx64m", "-cp", classpath, "com.stumbleupon.monitoring.jmx",
              vals[0]
              ], stdout=subprocess.PIPE).communicate()[0]

          if len(jmx) > 0:
            topologyPos=jmx.find("userevents:type=JmxMetricsConsumer")
            if topologyPos != -1:
              beans = [x.split("\t")[0] for x in jmx.split("\n")]
              #Check if there is a name
              topologyName="userevents"
              taskId=0

              for bean in beans:
                if bean.startswith('userevents'):
                  stormInfo=bean.split(',')
                  for stormDetail in stormInfo:
                    if stormDetail.startswith('name'):
                      topologyName=stormDetail.split('=')[1]
                    elif stormDetail.startswith('task'):
                      taskId=stormDetail.split('=')[1]

              t = Thread(target=processJMX, args=(vals[0], topologyName, taskId, classpath))
              t.daemon = True # thread dies with the program
              t.start()


    time.sleep(30)
    return 0  # Ask the tcollector to re-spawn us.


if __name__ == "__main__":
    sys.exit(main(sys.argv))


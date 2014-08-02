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

# How oftent to poll
INTERVAL="60"

# If this user doesn't exist, we'll exit immediately.
# If we're running as root, we'll drop privileges using this user.
USER = "voldemort"

# Use JAVA_HOME env variable if set
JAVA_HOME = '/usr/lib/jvm/j2sdk1.6-oracle'
JAVA = "%s/bin/java" % JAVA_HOME

# We add those files to the classpath if they exist.
CLASSPATH = [
    "%s/lib/tools.jar" % JAVA_HOME,
]

IGNORED_METRICS = set([ ])

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

    jpid = "VoldemortServer"
    output = subprocess.Popen(['/usr/bin/jps'], stdout=subprocess.PIPE).communicate()[0]
    jps = output.split("\n")
    for item in jps:
      vals = item.split(" ")
      if len(vals) == 2:
        if vals[1] == "VoldemortServer":
          jpid = vals[0]
          break

    jmx = subprocess.Popen(
        [JAVA, "-enableassertions", "-enablesystemassertions",  # safe++
         "-Xmx64m",
         "-cp", classpath, "com.stumbleupon.monitoring.jmx",
         "--watch", INTERVAL , "--long", "--timestamp",
         jpid,
         "voldemort.store.stats", 'numberOfCalls|InMs$',
         "Threading", "^ThreadCount|^PeakThreadCount",
         "OperatingSystem", "MaxFileDescriptorCount|OpenFileDescriptorCount",
         "GarbageCollector", "CollectionCount|CollectionTime"
         ], stdout=subprocess.PIPE, bufsize=1)
    print >>sys.stderr, "cmd: " ," ".join([JAVA, "-enableassertions", "-enablesystemassertions",  # safe++
         "-Xmx64m",
         "-cp", classpath, "com.stumbleupon.monitoring.jmx",
         "--watch", INTERVAL , "--long", "--timestamp",
         jpid,
         "voldemort.store.stats", 'numberOfCalls|InMs$',
         "Threading", "^ThreadCount|^PeakThreadCount",
         "OperatingSystem", "MaxFileDescriptorCount|OpenFileDescriptorCount",
         "GarbageCollector", "CollectionCount|CollectionTime"
         ])
    do_on_signal(signal.SIGINT, kill, jmx)
    do_on_signal(signal.SIGPIPE, kill, jmx)
    do_on_signal(signal.SIGTERM, kill, jmx)
    try:
        prev_timestamp = 0
        for line in jmx.stdout:
            if not line and jmx.poll() is not None:
                break  # Nothing more to read and process exited.
            elif len(line) < 4:
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

            tags = ""
            if metric.endswith("InMs"): 
                op = metric
                tags = " op=" + op.replace("average","").replace("TimeInMs","").replace("Completion","").lower()
                metric = "avgTime"
            elif metric.startswith("numberOfCallsTo"):
                op = metric
                tags = " op=" + op.replace("numberOfCallsTo","").lower()
                metric = "calls"

            # OS File descriptors for this process
            if metric.endswith("FileDescriptorCount"): 
                metric = "fd." + metric.replace("FileDescriptorCount","").lower()

            if metric.startswith("Collection"): 
                metric = "jvm.gc." + metric.replace("Collection","").lower()
                mbean_domain, mbean_properties = mbean.rstrip().split(":", 1)
                mbean_properties = dict(prop.split("=", 1)
                                    for prop in mbean_properties.split(","))
                gcname = mbean_properties.get("name")
                tags = tags + " collector=" + gcname 

            if metric.endswith("ThreadCount"): 
                metric = "jvm.threads." + metric.replace("PeakThreadCount","peak")
                metric = metric.replace("ThreadCount","current")

            # mbean is of the form "domain:key=value,...,foo=bar"
            mbean_domain, mbean_properties = mbean.rstrip().split(":", 1)
            mbean_properties = dict(prop.split("=", 1)
                                    for prop in mbean_properties.split(","))
            if mbean_domain.startswith("voldemort.store.stats"):
              storename = mbean_properties.get("type");
              tags = tags + " store=" + storename 

            metric = "voldemort." + metric


            sys.stdout.write("%s %d %s%s\n"
                             % (metric, timestamp, value, tags))
            sys.stdout.flush()
    finally:
        kill(jmx)
        time.sleep(300)
        return 0  # Ask the tcollector to re-spawn us.


if __name__ == "__main__":
    sys.exit(main(sys.argv))

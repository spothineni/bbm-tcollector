#!/usr/bin/python

#bytesSent       0       radiosite:type=GlobalRequestProcessor,name="http-bio-8543"
#bytesReceived   0       radiosite:type=GlobalRequestProcessor,name="http-bio-8543"
#errorCount      0       radiosite:type=GlobalRequestProcessor,name="http-bio-8543"
#requestCount    1       radiosite:type=GlobalRequestProcessor,name="http-bio-8543"
#processingTime  3       radiosite:type=GlobalRequestProcessor,name="http-bio-8543"
#maxTime 3       radiosite:type=GlobalRequestProcessor,name="http-bio-8543"

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
USER = "tomcat7"

# Use JAVA_HOME env variable if set
JAVA_HOME = '/usr/lib/jvm/java-6-sun'
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

    jpid = "Bootstrap"
    output = subprocess.Popen(['/usr/bin/jps'], stdout=subprocess.PIPE).communicate()[0]
    jps = output.split("\n")
    for item in jps:
      vals = item.split(" ")
      if len(vals) == 2:
        if vals[1] == "Bootstrap":
          jpid = vals[0]
          break

    jmx = subprocess.Popen(
        [JAVA, "-enableassertions", "-enablesystemassertions",  # safe++
         "-Xmx64m",
         "-cp", classpath, "com.stumbleupon.monitoring.jmx",
         "--watch", INTERVAL , "--long", "--timestamp",
         jpid,
         "type=GlobalRequestProcessor", 'requestCount|errorCount|^bytes|processingTime|maxTime' 
         ], stdout=subprocess.PIPE, bufsize=1)
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
            if metric.startswith("bytes"): 
                data = metric
                tags = " direction=" + data.replace("bytes","").lower()
                metric = "bytes"
            elif metric.endswith("Count"):
                req = metric
                tags = " status=" + req.replace("Count","").replace("request","ok").lower()
                metric = "request"
            elif metric.endswith("Time"):
                req = metric
                tags = " type=" + req.replace("Time","").replace("processing","total").lower()
                metric = "time"
            else:
                continue

            #print >>sys.stderr, ("metric %s\n" % metric)

            # mbean is of the form "domain:key=value,...,foo=bar"
            mbean_domain, mbean_properties = mbean.rstrip().split(":", 1)
            mbean_properties = dict(prop.split("=", 1)
                                    for prop in mbean_properties.split(","))
         
            tags = tags + " appplication=" + mbean_domain
            tags = tags + " port=" + mbean_properties.get("name").replace("http-bio-","").replace('"','');

            metric = "tomcat." + metric

            sys.stdout.write("%s %d %s%s\n"
                             % (metric, timestamp, value, tags))
            sys.stdout.flush()
    finally:
        kill(jmx)
        time.sleep(300)
        return 0  # Ask the tcollector to re-spawn us.


if __name__ == "__main__":
    sys.exit(main(sys.argv))

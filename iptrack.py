#!/usr/bin/python

import os
import subprocess
import re
import pwd
import sys
import time

from collectors.lib import utils

def main(unused_args):
    """procnettcp main loop"""
    try:           # On some Linux kernel versions, with lots of connections
      os.nice(19)  # this collector can be very CPU intensive.  So be nicer.
    except OSError, e:
      print >>sys.stderr, "warning: failed to self-renice:", e

    interval = 15

#   We need root to run the iptables command
#    utils.drop_privileges()

    prog = re.compile("^^\s*(?P<pkts>\d+)\s+(?P<bytes>\d+)\s+(?P<proto>tcp|udp|all).*?([sd]pt:(?P<port>\d+))?\s+/\* (?P<thing>[\d.]+) (?P<chain>\S+) (?P<junk>\S+) =munin\.iptrack(\[(?P<group>\w+)\])?\.(?P<name>\w+)\.(?P<dir>in|out)(=(?P<label>.*))?\s\*/$")

    while True:
      ts = int(time.time())
      metrics={"bytes":{"in": {}, "out":{}}, "packets": {"in": {}, "out":{}}} 
      for line in subprocess.check_output(["iptables", "-x", "-n", "-L", "-v"]).split('\n'):
        result = prog.match(line)
        if result != None:
          tags = ""
          tags += "name=" + result.group('name')
          if result.group('group') != None:
            tags += " group=" + result.group('group')
          tags += " proto=" + result.group('proto')
          if result.group('port') != None:
            tags += " port=" + result.group('port')

          # This results in too many tags
          #if result.group('chain') != None:
          #  tags += " chain=" + result.group('chain')

          if tags in metrics["bytes"][result.group('dir')]: 
            metrics["bytes"][result.group('dir')][tags] += result.group('bytes')
          else:
            metrics["bytes"][result.group('dir')][tags] = result.group('bytes')

          if tags in metrics["packets"][result.group('dir')]: 
            metrics["packets"][result.group('dir')][tags] += result.group('pkts')
          else:
            metrics["packets"][result.group('dir')][tags] = result.group('pkts')
          
          #result.group('label')
      for m in ["bytes", "packets"]:
        for d in ["in","out"]:
          for k in metrics[m][d].keys():
            print "iptrack.%s.%s %s %s %s" % (m,d,ts, metrics[m][d][k], k)

      time.sleep(interval)

if __name__ == "__main__":
    sys.exit(main(sys.argv))

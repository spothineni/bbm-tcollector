#!/usr/bin/python

import signal
import sys
import subprocess
from collectors.lib  import utils
from bbm import RunCollector
from bbm.jmx import start_jmx_collector

# These are the jmx handlers we'll be using.
from bbm.jvm import jvm_collector
from bbm.tomcat import tomcat_collector

signal.signal(signal.SIGCHLD, signal.SIG_IGN)

# The bbm-admin uses an embdedded tomcat with the webapp name set to "Tomcat", we';;
# rewrite the webapp name to "bbm-admin"
def renamer(v):
    if v.metric.startswith("tomcat."):
        v.tags = map(lambda t : "webapp=admin" if t.startswith("webapp=") else t , v.tags)
    return v

# Find the pid of the bbm-admin server
pgrep = subprocess.check_output(["/usr/bin/pgrep","-f", "-u", "bbm-admin", "/usr/share/bbm-admin/admin-assembly-1.0.jar"])
jpid = pgrep.rstrip("\n")
if jpid == "":
   sys.exit(1)

# We can change over to hte bbm-admin user for security
utils.drop_privileges(user="bbm-admin")

RunCollector(start_jmx_collector(15, jpid, jvm_collector + tomcat_collector, renamer), extraTags=["application=admin"])


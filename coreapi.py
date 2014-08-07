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
from bbm.c3p0 import c3p0_collector

signal.signal(signal.SIGCHLD, signal.SIG_IGN)

# The core-api uses an embdedded tomcat with the webapp name set to "Tomcat", we';;
# rewrite the webapp name to "coreapi" 
def renamer(v):
    if v.metric.startswith("tomcat."):
        v.tags = map(lambda t : "webapp=coreapi" if t.startswith("webapp=") else t , v.tags)
    return v

# Find the pid of the bbm-core-api server
pgrep = subprocess.check_output(["/usr/bin/pgrep","-u", "bbm-core-api", "java"])
jpid = pgrep.rstrip("\n") 
if jpid == "":
   sys.exit(1)

# We can change over to hte bbm-core-api user for secturity
utils.drop_privileges(user="bbm-core-api")

RunCollector(start_jmx_collector(15, jpid, jvm_collector + tomcat_collector + c3p0_collector, renamer), extraTags=["application=coreapi"])


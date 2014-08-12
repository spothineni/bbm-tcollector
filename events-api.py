#!/usr/bin/python

import signal
import sys
import subprocess
from collectors.lib  import utils
from bbm import RunCollector
from bbm.jmx import start_jmx_collector

# These are the jmx handlers we'll be using.
from bbm.jvm import jvm_collector
from bbm.jetty import jetty_collector

signal.signal(signal.SIGCHLD, signal.SIG_IGN)

# Find the pid of the tomcat server
pgrep = subprocess.check_output(["/usr/bin/pgrep","-u", "bbm-events-api", "-f", "/etc/bbm/bbm-events-api.yml"])
jpid = pgrep.rstrip("\n") 
if jpid == "":
   sys.exit(1)

# We can change over to tomcat7 user for secturity
utils.drop_privileges(user="bbm-events-api")

RunCollector(start_jmx_collector(15, jpid, jvm_collector + jetty_collector), extraTags=["application=events-api"])


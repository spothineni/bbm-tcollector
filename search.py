#!/usr/bin/python

import signal
import sys
import subprocess
from collectors.lib  import utils
from bbm import RunCollector
from bbm.jmx import start_jmx_collector

# These are the jmx handlers we'll be using.
from bbm.jvm import jvm_collector

signal.signal(signal.SIGCHLD, signal.SIG_IGN)

# Find the pid of the bbm-core-api server
pgrep = subprocess.check_output(["/usr/bin/pgrep","-f","-u", "bbm-search", "/usr/share/bbm-search/bbm-search-all.jar"])
jpid = pgrep.rstrip("\n")
if jpid == "":
   sys.exit(1)

# We can change over to hte bbm-core-api user for secturity
utils.drop_privileges(user="bbm-search")

RunCollector(start_jmx_collector(15, jpid, jvm_collector), extraTags=["application=search"])


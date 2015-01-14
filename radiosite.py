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

# The radiosite uses an embdedded tomcat with the webapp name set to "Tomcat", we';;
# rewrite the webapp name to "radiosite"
def renamer(v):
    if v.metric.startswith("tomcat."):
        v.tags = map(lambda t : "webapp=radiosite" if t.startswith("webapp=") else t , v.tags)
    return v

# Find the pid of the bbm-radio-site server
pgrep = subprocess.check_output(["/usr/bin/pgrep","-f","-u", "bbm-radio-site", "/usr/share/bbm-radio-site/radio-site.war"])
jpid = pgrep.rstrip("\n")
if jpid == "":
   sys.exit(1)

# We can change over to hte bbm-radio-site user for security
utils.drop_privileges(user="bbm-radio-site")

RunCollector(start_jmx_collector(15, jpid, jvm_collector + tomcat_collector, renamer), extraTags=["application=radiosite"])


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

# Find the pid of the tomcat server
pgrep = subprocess.check_output(["/usr/bin/pgrep","-u", "tomcat7", "java"])
jpid = pgrep.rstrip("\n") 
if jpid == "":
   sys.exit(1)

# We can change over to tomcat7 user for secturity
utils.drop_privileges(user="tomcat7")

RunCollector(start_jmx_collector(15, jpid, jvm_collector + tomcat_collector, extraTags=["application=tomcat7"])


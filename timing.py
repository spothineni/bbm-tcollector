#!/usr/bin/python

import time
import signal
import sys
import re
import urlparse
import string

from collectors.lib  import utils
from bbm import QUEUE_FINISHED, TSDBMetricData, RunCollector
from bbm.file import start_dated_files_collector

signal.signal(signal.SIGCHLD, signal.SIG_IGN)

# May  6 07:45:50 10.0.2.108 S=1430894750368|UUID=5549B89E-5D5D82CE0050-BC1DA471D157-24D3-1577D3D0|DB=7|MCache=4|TXN=5|Time=22|SID=4A003FEA3243055880ECA85FEF8C5A3A|IP=188.29.164.113|C=GB|U=452401398|H=alke.int|M=GET|SC=200|URL=/api/0.1/lyrics/snippet?allowExplicit=true&apiKey=android&appVersion=1&format=json&trackVersion=15429919
class LogParser:
    oldtime = 0
    call_counts = {}
    time_total = {}
    time_max = {}
    memc_total = {}
    memc_max = {}
    db_total = {}
    db_max = {}
    txns_total = {}
    txns_max = {}

    def ParseLine(self, v):
        numMatch = re.compile("[,\d]+")
        uuidMatch = re.compile("[A-Z0-9]+(-[A-Z0-9]+)+")
        fields = v.split('|')
        fields = fields[1:]

        url = ""
        path = ""
        key = "none"
        method = "UNKNOWN"
        SC = "UNKNOWN"
        db = 0
        Time = 0
        TXN = 0
        MCache = 0

        try:
            url = fields[len(fields) - 1].split('=',1)[1]
            parts = url.split('?',1)
            path = parts[0]
            qs = ""
            if len(parts) > 1:
                qs = parts[1]
            if path.startswith('/rest'):
                path = numMatch.sub("M",path)
                path = uuidMatch.sub("UUID",path)
            apikeys =  [s for s in qs.split("&") if s.startswith("apiKey=")]
            if len(apikeys) > 0:
                key = apikeys[0][len("apikey="):]

            for kv in fields:
                if kv.startswith("DB="):
                    db = int(kv[len("DB="):])
                elif kv.startswith("Time="):
                    Time = int(kv[len("Time="):])
                elif kv.startswith("TXN="):
                    TXN = int(kv[len("TXN="):])
                elif kv.startswith("MCache="):
                    MCache = int(kv[len("MCache="):])
                elif kv.startswith("M="):
                    method = kv[len("M="):]
                elif kv.startswith("SC="):
                    SC = kv[len("SC="):]

        except Exception as e:
            print e
            return []

        path = path.replace("(null)","NULL")
        tag = "path=%s key=%s method=%s" %(path, key, method)
        if tag in self.call_counts:
          self.call_counts[tag] = self.call_counts[tag] + 1
        else:
          self.call_counts[tag] = 1

        if tag in self.time_total:
          self.time_total[tag] = self.time_total[tag] + Time
        else:
          self.time_total[tag] = Time
        
        if tag in self.time_max:
          if Time > self.time_max[tag]:
              self.time_max[tag] = Time
        else:
          self.time_max[tag] = Time

        if tag in self.db_total:
          self.db_total[tag] = self.db_total[tag] + db
        else:
          self.db_total[tag] = db
        
        if tag in self.db_max:
          if Time > self.db_max[tag]:
              self.db_max[tag] = db
        else:
          self.db_max[tag] = db
        
        if tag in self.memc_total:
          self.memc_total[tag] = self.memc_total[tag] + MCache
        else:
          self.memc_total[tag] = MCache
        
        if tag in self.memc_max:
          if Time > self.memc_max[tag]:
              self.memc_max[tag] = MCache
        else:
          self.memc_max[tag] = MCache

        if tag in self.txns_total:
          self.txns_total[tag] = self.txns_total[tag] + TXN
        else:
          self.txns_total[tag] = TXN
        
        if tag in self.txns_max:
          if TXN > self.txns_max[tag]:
              self.txns_max[tag] = TXN
        else:
          self.txns_max[tag] = TXN

        data = [] 

        newtime = int(round(time.time() * 1000))
        if (newtime - self.oldtime) >= 5000:
            for tag in self.call_counts.keys():
                data = data + [TSDBMetricData("timings.calls", self.call_counts[tag],tag.split(" "))]
            for tag in self.time_total.keys():
                data = data + [TSDBMetricData("timings.duration.total", self.time_total[tag],tag.split(" "))]
            for tag in self.time_max.keys():
                data = data + [TSDBMetricData("timings.duration.max", self.time_max[tag],tag.split(" "))]
            for tag in self.db_total.keys():
                data = data + [TSDBMetricData("timings.db.total", self.db_total[tag],tag.split(" "))]
            for tag in self.db_max.keys():
                data = data + [TSDBMetricData("timings.db.max", self.db_max[tag],tag.split(" "))]
            for tag in self.memc_total.keys():
                data = data + [TSDBMetricData("timings.memcache.total", self.memc_total[tag],tag.split(" "))]
            for tag in self.memc_max.keys():
                data = data + [TSDBMetricData("timings.memcache.max", self.memc_max[tag],tag.split(" "))]
            for tag in self.txns_total.keys():
                data = data + [TSDBMetricData("timings.txns.total", self.txns_total[tag],tag.split(" "))]
            for tag in self.txns_max.keys():
                data = data + [TSDBMetricData("timings.txns.max", self.txns_max[tag],tag.split(" "))]
            self.time_max = {}
            self.db_max = {}
            self.txns_max = {}
            self.memc_max = {}
            self.oldtime = newtime
#            for tag in self.ends_hash.keys():
#                data = data + [TSDBMetricData("streams.duration", self.ends_hash[tag],tag.split(" "))]
#
#        if (newtime - self.olduserstime) >= (1000 * 60 * 5): # Only output users stats every 5 minutes
#            for tag in self.users_hash.keys():
#                data = data + [TSDBMetricData("streams.users.5min", len(self.users_hash[tag]),tag.split(" "))]
#            self.users_hash = {}
#            self.olduserstime = newtime

        return data

utils.drop_privileges(user="nobody")

parser = LogParser()
RunCollector(start_dated_files_collector("/var/log/java", "*/*/*-timing.log","%Y/%m/%Y%m%d-timing.log",parser.ParseLine), exitOnFinished=False)

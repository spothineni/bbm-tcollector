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

#dln.www.we7.com 2.97.76.192 - - [10/Jul/2014:15:04:43 +0100] "GET /stream/musicSrc/0223ea2a17294bbd91671e494db72146_aac64-PRCD-517542_0001_00001_LL_64k_full.m4a?streamunique=a215f073-cc32-472a-b43d-fc4c63ff45b2&e=1405001683&st=etUnU2Yb78_g7MZzdkVWTA HTTP/1.1" 206 37231 (bytes 0-1730541/1730542) "-" "AppleCoreMedia/1.0.0.11D257 (iPhone; U; CPU OS 7_1_2 like Mac OS X; en_us)" HIT
#dln.www.we7.com 2.219.210.196 - - [10/Jul/2014:15:04:44 +0100] "GET /stream/musicSrc/d054e6df6f3141d7a1665ef99fff7557_1116254170033.mp3?streamunique=a4484afa-81b9-4165-b23e-98ce07c7d05b&e=1405001680&st=BEzGpYB1eXyfEXlgK_g8mw HTTP/1.1" 200 5738369 (-) "-" "blinkbox%20music/3.5.1 CFNetwork/672.1.15 Darwin/14.0.0" HIT


nginxRegex = re.compile("^(dl[a-zA-Z0-9\.]+) [\d\.]+ - - \[[^\]]*\] \"([A-Z]+) ([^\"]*) [A-Z/\.[0-9]+\" ([0-9]+) ([0-9\-]+) \(([^\)]+)\) .* ([A-Z\.]+)$")
rangeRegex = re.compile("^[a-zA-Z]+ +(\d+)-(\d+)/(\d+)$")

class LogParser:
    oldtime = 0

    requests_hash = {}
    bytessent_hash = {}
    ranges_hash = {}
    min_bytessent_hash = {}
    max_bytessent_hash = {}

    def ParseLine(self, v):
        fields = nginxRegex.match(v)
        if fields == None:
            return []
        try:
            http_host, http_method, http_req, http_status, http_bytes, http_range, cache_state = fields.groups()

            url         = http_req.split('?')
            http_path   = url[0]
            fileformat  = "none"
            if http_path.startswith("/stream/musicSrc/"):
                comps = http_path.split(".")
                if len(comps) > 1:
                    fileformat = comps[-1]
            
            if cache_state != "HIT" and cache_state != "MISS":
                cache_state = "UNKNOWN"

            tag = "http_host=%s http_status=%s format=%s cache_state=%s" % (
                  http_host,
                  http_status,
                  fileformat,
                  cache_state,
                  )

            if http_range != "-":
                rangefields = rangeRegex.match(http_range)
                if rangefields != None:
                    start, end, full = rangefields.groups()
                    percentage = ((float(end) - float(start)) / float(full)) * 100 
                    low = int(percentage) / 10
                    high = low + 1
                    # http_status and http_host are pointless for this metric
                    # so we track different tags
                    http_range_tag = "format=%s cache_state=%s http_range=%s0-%s0" %(fileformat, cache_state, low, high)
                    if http_range_tag in self.ranges_hash:
                        self.ranges_hash[http_range_tag] += 1
                    else:
                        self.ranges_hash[http_range_tag] = 1
            
            if tag in self.requests_hash:
                self.requests_hash[tag] += 1
            else:
                self.requests_hash[tag] = 1

            if http_bytes.isdigit():
                val = int(http_bytes)
                if tag in self.bytessent_hash:
                    self.bytessent_hash[tag] += val
                else:
                    self.bytessent_hash[tag] = val

                if tag in self.min_bytessent_hash:
                    self.min_bytessent_hash[tag] = min(self.min_bytessent_hash[tag], val) 
                else:
                    self.min_bytessent_hash[tag] = val

                if tag in self.max_bytessent_hash:
                    self.max_bytessent_hash[tag] = max(self.max_bytessent_hash[tag], val) 
                else:
                    self.max_bytessent_hash[tag] = val
          

        except Exception as e:
            print >>sys.stderr, e
            return []
                  
        newtime = int(round(time.time() * 1000))

        if (newtime - self.oldtime) >= 5000:
            data = []
            for tag in self.requests_hash.keys():
                data = data + [TSDBMetricData("nginx.streams.requests", self.requests_hash[tag] ,tag.split(" "))]
            for tag in self.ranges_hash.keys():
                data = data + [TSDBMetricData("nginx.streams.range_requests", self.ranges_hash[tag] ,tag.split(" "))]
            for tag in self.bytessent_hash.keys():
                data = data + [TSDBMetricData("nginx.streams.bytes_sent", self.bytessent_hash[tag] ,tag.split(" "))]
            for tag in self.min_bytessent_hash.keys():
                data = data + [TSDBMetricData("nginx.streams.bytes_sent_min", self.min_bytessent_hash[tag] ,tag.split(" "))]
            for tag in self.max_bytessent_hash.keys():
                data = data + [TSDBMetricData("nginx.streams.bytes_sent_max", self.max_bytessent_hash[tag] ,tag.split(" "))]
            self.oldtime = newtime
            self.min_bytessent_hash = {}
            self.max_bytessent_hash = {}

            return data
        else: 
            return  []

utils.drop_privileges(user="nobody")

parser = LogParser()
RunCollector(start_dated_files_collector("/var/log/nginx", "*-streams.log","%Y%m%d-streams.log",parser.ParseLine), exitOnFinished=False)

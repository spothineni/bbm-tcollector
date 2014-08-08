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

#Aug  5 19:59:54 iris pound: api.stagingb.we7.com 93.93.134.11 - - [05/Aug/2014:19:59:54 +0100] "GET /api/0.1/user/anonymousUserId?apiKey=radiosite&appVersion=1&locale=en_US HTTP/1.1" 200 47 "" "we7 HttpClient hulk-5-1406558659 (we7-no-session)" (- -> 10.0.9.123:8080) 0.009 sec

poundRegex = re.compile("^.+ pound: ([a-zA-Z0-9\.:]+) [\d\.]+ - - \[[^\]]*\] \"([A-Z]+) ([^\"]*) [A-Z/\.[0-9]+\" ([0-9]+) ([0-9\-]+) .* ([0-9\.]+) sec$")

class LogParser:
    oldtime = 0
    requests_hash = {}
    bytessent_hash = {}
    min_bytessent_hash = {}
    max_bytessent_hash = {}
    duration_hash = {}
    min_duration_hash = {}
    max_duration_hash = {}

    def ParseLine(self, v):
        fields = poundRegex.match(v)
        if fields == None:
            return []
        try:
            http_host, http_method, http_req, http_status, http_bytes, http_duration = fields.groups()

            url         = http_req.split('?')
            http_path   = url[0]
            api_key     = "none"
            if len(url) > 1:
                http_args   = urlparse.parse_qs(url[1])
                if "apiKey" in http_args:
                    api_key = http_args['apiKey'][0]
            
            http_port=False

            if ':' in http_host:
                host_port = string.split(http_host,':',1)
                http_host = host_port[0]
                http_port = host_port[1]
            else:
                http_port = "80"

            tag = "http_host=%s http_status=%s http_method=%s" % (
                  http_host,
                  http_status,
                  http_method,
                  )
            
            # We need to indicate if we have no api key, or we get
            # overlapping duplicate tsdb series
            if api_key != "":
                tag = tag + " api_key=" + api_key
            else:
                tag = tag + " api_key=none"

            if http_port != False:
                tag = tag + " http_port=" + http_port

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
          
            val = float(http_duration)
            if tag in self.duration_hash:
                self.duration_hash[tag] += val
            else:
                self.duration_hash[tag] = val

            if tag in self.min_duration_hash:
                self.min_duration_hash[tag] = min(self.min_duration_hash[tag], val) 
            else:
                self.min_duration_hash[tag] = val

            if tag in self.max_duration_hash:
                self.max_duration_hash[tag] = max(self.max_duration_hash[tag], val) 
            else:
                self.max_duration_hash[tag] = val

        except Exception as e:
            print >>sys.stderr, e
            return []
                  
        newtime = int(round(time.time() * 1000))

        if (newtime - self.oldtime) >= 5000:
            data = []
            for tag in self.requests_hash.keys():
                data = data + [TSDBMetricData("poundlogs.requests", self.requests_hash[tag] ,tag.split(" "))]
            for tag in self.bytessent_hash.keys():
                data = data + [TSDBMetricData("poundlogs.bytes_sent", self.bytessent_hash[tag] ,tag.split(" "))]
            for tag in self.min_bytessent_hash.keys():
                data = data + [TSDBMetricData("poundlogs.bytes_sent_min", self.min_bytessent_hash[tag] ,tag.split(" "))]
            for tag in self.max_bytessent_hash.keys():
                data = data + [TSDBMetricData("poundlogs.bytes_sent_max", self.max_bytessent_hash[tag] ,tag.split(" "))]
            for tag in self.duration_hash.keys():
                data = data + [TSDBMetricData("poundlogs.duration", self.duration_hash[tag] ,tag.split(" "))]
            for tag in self.min_duration_hash.keys():
                data = data + [TSDBMetricData("poundlogs.duration_min", self.min_duration_hash[tag] ,tag.split(" "))]
            for tag in self.max_duration_hash.keys():
                data = data + [TSDBMetricData("poundlogs.duration_max", self.max_duration_hash[tag] ,tag.split(" "))]
            self.oldtime = newtime
            self.min_duration_hash = {}
            self.max_duration_hash = {}
            self.min_bytessent_hash = {}
            self.max_bytessent_hash = {}

            return data
        else: 
            return  []


utils.drop_privileges(user="nobody")

parser = LogParser()
RunCollector(start_dated_files_collector("/var/log/pound", "*/*/*-pound.log","%Y/%m/%Y%m%d-%H00-pound.log",parser.ParseLine), exitOnFinished=False)

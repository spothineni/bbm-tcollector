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

class LogParser:
    oldtime = 0

    starts_hash = {}

    ends_hash = {}
    ends_hash['type=played'] = 0
    ends_hash['type=paused'] = 0

    olduserstime = 0
    users_hash = {}

    def ParseLine(self, v):
        fields = v.split()
        if len(fields) == 5:
            logdata = fields[4].split(':',1)
            call = ""
            data = ""
            if len(logdata) == 1:
              call = "StreamInfo"
              data = logdata[0]
            elif len(logdata) == 2:
              if logdata[0] == "RadioReport":
                call,data = logdata[1].split(':',1)
              else:
                call = logdata[0]
                data = logdata[1]
            else:
              return []

            records = {}
            try:
                for r in data.split('|'):
                     k,v = r.split('=',1)
                     records[k] = v
            except:
                return []

            if call == 'StreamInfo' or call == 'StreamStart':
                tag = "key=%s country=%s source=%s" % (
                    records['KEY'],
                    records['COUNTRY'],
                    records['SOURCE'])
                if tag in self.starts_hash:
                    self.starts_hash[tag] += 1
                else:
                    self.starts_hash[tag] = 1

                if tag in self.users_hash:
                    self.users_hash[tag][records['USERID']] = True
                else:
                    self.users_hash[tag] = {}
                    self.users_hash[tag][records['USERID']] = True
            elif call.startswith('StreamEnd'):
                if 'PLAYEDTIME' in records:
                    self.ends_hash['type=played'] += int(records['PLAYEDTIME'])
                if 'PAUSEDTIME' in records:
                    self.ends_hash['type=paused'] += int(records['PAUSEDTIME'])

        data = []

        newtime = int(round(time.time() * 1000))
        if (newtime - self.oldtime) >= 5000:
            for tag in self.starts_hash.keys():
                data = data + [TSDBMetricData("streams.requests", self.starts_hash[tag],tag.split(" "))]
            for tag in self.ends_hash.keys():
                data = data + [TSDBMetricData("streams.duration", self.ends_hash[tag],tag.split(" "))]
            self.oldtime = newtime

        if (newtime - self.olduserstime) >= (1000 * 60 * 5): # Only output users stats every 5 minutes
            for tag in self.users_hash.keys():
                data = data + [TSDBMetricData("streams.users.5min", len(self.users_hash[tag]),tag.split(" "))]
            self.users_hash = {}
            self.olduserstime = newtime

        return data

utils.drop_privileges(user="nobody")

parser = LogParser()
RunCollector(start_dated_files_collector("/var/log/java", "*/*/*-streams.log","%Y/%m/%Y%m%d-streams.log",parser.ParseLine), exitOnFinished=False)

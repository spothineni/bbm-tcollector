#!/usr/bin/env python

# -----------------------------------------------------------------------------
# blinkbox music haproxy TCollector Plugin
# by Jonathan Wright <jonathanw@blinkbox.com> (c) 2014
#
# TCollector plugin for HAProxy and OpenTSDB, fetching frontend, backend, and
# server information from the unix socket.
# -----------------------------------------------------------------------------
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.  This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
# General Public License for more details.  You should have received a copy
# of the GNU Lesser General Public License along with this program.  If not,
# see <http://www.gnu.org/licenses/>.
# -----------------------------------------------------------------------------
# Changelog:
#   1.0.0   - First Release

"""HAProxy Collector for OpenTSDB"""

import socket
import sys
import time
import re
from collectors.lib import utils

COLLECTION_INTERVAL = 15
DEFAULT_SOCKET = '/var/run/haproxy.sock'

STAT_PROXY_NAME = 0
STAT_SERVICE_NAME = 1
STAT_QUEUED_REQUESTS = 2
STAT_QUEUED_MAX = 3
STAT_SESSIONS_CURRENT = 4
STAT_SESSIONS_MAX = 5
STAT_SESSIONS_LIMIT = 6
STAT_CONNECTIONS_TOTAL = 7
STAT_BYTES_IN = 8
STAT_BYTES_OUT = 9
STAT_REQUESTS_DENIED = 10
STAT_RESPONSES_DENIED = 11
STAT_REQUESTS_ERROR = 12
STAT_CONNECTIONS_ERROR = 13
STAT_RESPONSES_ERROR = 14
STAT_CONNECTIONS_RETRIED = 15
STAT_CONNECTIONS_REDISPATCHED = 16
STAT_STATUS = 17
STAT_WEIGHT = 18
STAT_SERVER_ACTIVE = 19
STAT_SERVERS_ACTIVE = 19
STAT_SERVER_BACKUP = 20
STAT_SERVERS_BACKUP = 20
STAT_CHECKS_FAIL = 21
STAT_CHECKS_GO_DOWN = 22
STAT_CHECKS_LAST_CHANGE = 23
STAT_CHECKS_DOWNTIME = 24
STAT_QUEUED_LIMIT = 25
STAT_PID = 26
STAT_UID = 27
STAT_SID = 28
STAT_THROTTLE = 29
STAT_SESSIONS_TOTAL = 30
STAT_TRACKED = 31
STAT_SERVICE_TYPE = 32
STAT_SESSIONS_RATE_CURRENT = 33
STAT_SESSIONS_RATE_LIMIT = 34
STAT_SESSIONS_RATE_MAX = 35
STAT_CHECK_STATUS = 36
STAT_CHECK_CODE = 37
STAT_CHECK_DURATION = 38
STAT_RESPONSES_HTTP_1XX = 39
STAT_RESPONSES_HTTP_2XX = 40
STAT_RESPONSES_HTTP_3XX = 41
STAT_RESPONSES_HTTP_4XX = 42
STAT_RESPONSES_HTTP_5XX = 43
STAT_RESPONSES_HTTP_XXX = 44
STAT_CHECK_FAILED_DETAILS = 45
STAT_REQUESTS_RATE_CURRENT = 46
STAT_REQUESTS_RATE_MAX = 47
STAT_REQUESTS_TOTAL = 48
STAT_ABORTS_CLIENT = 49
STAT_ABORTS_SERVER = 50
STAT_COMPRESSOR_IN = 51
STAT_COMPRESSOR_OUT = 52
STAT_COMPRESSOR_BYPASSED = 53
STAT_COMPRESSOR_REQUESTS = 54
STAT_SESSIONS_LAST = 55
STAT_CHECK_HEALTH_LAST = 56
STAT_CHECK_AGENT_LAST = 57
STAT_TIME_QUEUE = 58
STAT_TIME_CONNECT = 59
STAT_TIME_RESPONSE = 60
STAT_TIME_TOTAL = 61
# Types used by HAProxy for some fields
TYPE_FRONTEND = 0
TYPE_BACKEND = 1
TYPE_SERVER = 2
TYPE_LISTENER = 3

def read_socket(sock):
    """
    Connect to the HAProxy stats socket and ready the data from the show stat
    command, allowing up to three retries before aborting. This setup assumes
    that the socket will be closed and doesn't try to keep it open, reconnecting
    on each attempt to fetch the statistics. (Should better handle restarts
    and reloads of the monitored process.)
    """

    stats = ''

    # Establish a socket to connect to the unix socket on HAProxy
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(DEFAULT_SOCKET)

    for attempt in range(3):
        try:
            sock.send("show stat\n")
            data = sock.recv(4096)
            while data:
                stats += data
                data = sock.recv(4096)
            return stats.split("\n")
        except IOError, error:
            utils.err("Error: Connection to HAProxy socket lost: %s (%d)"
                        % (error, attempt))
            sock.close()
            sock.connect(DEFAULT_SOCKET)
            # Reset stats in case it was broken mid-stream
            stats = ''

    # If all attempts fail, return empty array
    return []

def read_stats(stats):
    """
    Read the statistics provided by HAProxy and convert them into a pair of
    arrays for frontend types and backend types (with their servers) which
    can then be used to output the data to OpenTSDB.
    """

    fe = {}
    be = {}

    for line in stats:
        # Ignore empty or 'commented' lines (like the header)
        if len(line) == 0 or line[:2] == '# ':
            continue
        csv = read_line(line)

        # Create some shorthand variables to make the code more readable
        proxy = csv[STAT_PROXY_NAME]
        server = csv[STAT_SERVICE_NAME]

        if csv[STAT_SERVICE_TYPE] == TYPE_FRONTEND:
            fe[proxy] = {
                'sessions.current': csv[STAT_SESSIONS_CURRENT],
                'requests.denied': csv[STAT_REQUESTS_DENIED],
                'requests.errors': csv[STAT_REQUESTS_ERROR],
                'requests.total': csv[STAT_REQUESTS_TOTAL],
                'requests.http1xx': csv[STAT_RESPONSES_HTTP_1XX],
                'requests.http2xx': csv[STAT_RESPONSES_HTTP_2XX],
                'requests.http3xx': csv[STAT_RESPONSES_HTTP_3XX],
                'requests.http4xx': csv[STAT_RESPONSES_HTTP_4XX],
                'requests.http5xx': csv[STAT_RESPONSES_HTTP_5XX],
                'requests.httpxxx': csv[STAT_RESPONSES_HTTP_XXX],
                'connections.total': csv[STAT_CONNECTIONS_TOTAL],
                'bandwidth.in': csv[STAT_BYTES_IN],
                'bandwidth.out': csv[STAT_BYTES_OUT],
            }

        elif csv[STAT_SERVICE_TYPE] == TYPE_BACKEND:
            # As the backend total comes after each of the servers, these values
            # will need to be added into an already existing dict, or we'll
            # remove all the server data previously added
            be[proxy]['sessions.current'] = csv[STAT_SESSIONS_CURRENT]
            be[proxy]['requests.queued'] = csv[STAT_QUEUED_REQUESTS]
            be[proxy]['responses.errors'] = csv[STAT_RESPONSES_ERROR]
            be[proxy]['connections.total'] = csv[STAT_CONNECTIONS_TOTAL]
            be[proxy]['connections.redispatched'] = \
                csv[STAT_CONNECTIONS_REDISPATCHED]
            be[proxy]['connections.retried'] = \
                csv[STAT_CONNECTIONS_RETRIED]
            be[proxy]['connections.error'] = \
                csv[STAT_CONNECTIONS_ERROR]
            be[proxy]['timing.queue'] = csv[STAT_TIME_QUEUE]
            be[proxy]['timing.connect'] = csv[STAT_TIME_CONNECT]
            be[proxy]['timing.response'] = csv[STAT_TIME_RESPONSE]
            be[proxy]['timing.total'] = csv[STAT_TIME_TOTAL]
            be[proxy]['bandwidth.in'] = csv[STAT_BYTES_IN]
            be[proxy]['bandwidth.out'] = csv[STAT_BYTES_OUT]
            be[proxy]['abort.client'] = csv[STAT_ABORTS_CLIENT]
            be[proxy]['abort.server'] = csv[STAT_ABORTS_SERVER]

        elif csv[STAT_SERVICE_TYPE] == TYPE_SERVER:
            # If the backend is new, do some basic preparation first to ensure
            # all the required default values are present
            if proxy not in be:
                be[proxy] = {
                    'servers': {},
                    'count.backup': 0,
                    'count.up': 0,
                    'count.disabled': 0,
                    'count.down': 0,
                }

            if not csv[STAT_SERVER_BACKUP]:
                be[proxy]['servers'][server] = {
                    'sessions.current': csv[STAT_SESSIONS_CURRENT],
                    'requests.queued': csv[STAT_QUEUED_REQUESTS],
                    'responses.total': csv[STAT_RESPONSES_HTTP_1XX] +
                                      csv[STAT_RESPONSES_HTTP_2XX] +
                                      csv[STAT_RESPONSES_HTTP_3XX] +
                                      csv[STAT_RESPONSES_HTTP_4XX] +
                                      csv[STAT_RESPONSES_HTTP_5XX] +
                                      csv[STAT_RESPONSES_HTTP_XXX],
                    'responses.http1xx': csv[STAT_RESPONSES_HTTP_1XX],
                    'responses.http2xx': csv[STAT_RESPONSES_HTTP_2XX],
                    'responses.http3xx': csv[STAT_RESPONSES_HTTP_3XX],
                    'responses.http4xx': csv[STAT_RESPONSES_HTTP_4XX],
                    'responses.http5xx': csv[STAT_RESPONSES_HTTP_5XX],
                    'responses.httpxxx': csv[STAT_RESPONSES_HTTP_XXX],
                    'responses.errors': csv[STAT_RESPONSES_ERROR],
                    'connections.total': csv[STAT_CONNECTIONS_TOTAL],
                    'connections.redispatched':
                        csv[STAT_CONNECTIONS_REDISPATCHED],
                    'connections.retried': csv[STAT_CONNECTIONS_REDISPATCHED],
                    'connections.error': csv[STAT_CONNECTIONS_ERROR],
                    'timing.queue': csv[STAT_TIME_QUEUE],
                    'timing.connect': csv[STAT_TIME_CONNECT],
                    'timing.response': csv[STAT_TIME_RESPONSE],
                    'timing.total': csv[STAT_TIME_TOTAL],
                    'timing.check': csv[STAT_CHECK_DURATION],
                    'abort.client': csv[STAT_ABORTS_CLIENT],
                    'abort.server': csv[STAT_ABORTS_SERVER],
                    # 'bandwidth.in': csv[STAT_BYTES_IN],
                    # 'bandwidth.out': csv[STAT_BYTES_OUT],
                }

            # Increment the backend count depending on the state of the server
            if csv[STAT_SERVER_BACKUP]:
                be[proxy]['count.backup'] += 1
            elif csv[STAT_STATUS] == 'UP':
                be[proxy]['count.up'] += 1
            elif re.match('^(MAINT|DRAIN|NOLB)', csv[STAT_STATUS]):
                be[proxy]['count.disabled'] += 1
            else:
                be[proxy]['count.down'] += 1

    return (fe, be)

def read_line(line):
    """
    Take a CSV line and split the values, converting any numeric values
    (including negative numbers) into integers before returning.
    """
    is_number = re.compile(r'-?\d+')
    values = []
    for value in line.split(','):
        if is_number.match(value):
            values.append(int(value))
        else:
            values.append(value)
    return values

def print_frontend(stats, timestamp):
    """
    Take the data from read_stats and output the FRONTEND data in the
    appropriate format.
    """

    for frontend, records in stats.iteritems():
        for key, value in records.iteritems():
            print("haproxy.frontend.%s %d %d frontend=%s"
                    % (key, timestamp, value, frontend))

def print_backend(stats, timestamp):
    """
    Take the data from read_stats and output the BACKEND and SERVER data in the
    appropriate format.
    """

    for backend, records in stats.iteritems():
        for key, value in records.iteritems():
            if key == 'servers':
                for server, records_ in value.iteritems():
                    for key_, value_ in records_.iteritems():
                        print(
                            "haproxy.server.%s %d %d backend=%s server=%s"
                                % (key_, timestamp, value_, backend, server))
            else:
                print("haproxy.backend.%s %d %d backend=%s"
                        % (key, timestamp, value, backend))

def collect_stats(sock):
    """Run the fetch, process, output cycle."""
    timestamp = time.time()
    # Fetch the stats from the socket
    stats = read_socket(sock)
    # Then pass for processing
    (fe, be) = read_stats(stats)
    # And then print out
    print_frontend(fe, timestamp)
    print_backend(be, timestamp)

def main():
    """Start processing HAProxy stats"""

    while True:
        try:
            collect_stats(DEFAULT_SOCKET)
            time.sleep(COLLECTION_INTERVAL)
        except KeyboardInterrupt:
            return 0

if __name__ == "__main__":
    sys.exit(main())

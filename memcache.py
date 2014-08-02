#!/usr/bin/env python

import socket
import subprocess
import sys
import time


COLLECTION_INTERVAL = 15  # seconds

# Those are the stats we MUST collect at every COLLECTION_INTERVAL.
IMPORTANT_STATS = [
  "rusage_user", "rusage_system",
  "curr_connections", "total_connections", "connection_structures",
  "cmd_get", "cmd_set",
  "get_hits", "get_misses",
  "delete_misses", "delete_hits",
  "bytes_read", "bytes_written", "bytes",
  "curr_items", "total_items", "evictions",
  ]
IMPORTANT_STATS_SET = set(IMPORTANT_STATS)

# Important things on a slab basis
IMPORTANT_STATS_SLAB = [
  "cas_hits", "cas_badval", "incr_hits", "decr_hits", "delete_hits",
  "cmd_set", "get_hits", "free_chunks", "used_chunks", "total_chunks"
]
IMPORTANT_STATS_SLAB_SET = set(IMPORTANT_STATS_SLAB)

# Stats that really don't belong to the TSDB.
IGNORED_STATS_SET = set(["time", "uptime", "version", "pid"])
IGNORED_STATS_SLAB_SET = set(["chunk_size", "chunks_per_page", "total_pages",
        "mem_requested", "free_chunks_end"])

# TODO(tsuna): Don't hardcode those.
DATASETS = {
  11211: "default",  # XXX StumbleUpon specific mapping of port-to-dataset
  }


def find_memcached():
  """Yields all the ports that memcached is listening to, according to ps."""
  p = subprocess.Popen(["pgrep", "-lf", "memcached"], stdout=subprocess.PIPE)
  stdout, stderr = p.communicate()
  assert p.returncode in (0, 1), "pgrep returned %r" % p.returncode
  for line in stdout.split("\n"):
    if not line:
      continue

    host = line.find(" -l ")
    if host < 0:
        host="127.0.0.1"
    else:
        host = line[host + 4:].split(" ")[0]
    #host = socket.inet_pton(socket.AF_INET,host)

    port = line.find(" -p ")
    if port < 0:
      print >>sys.stderr, "Weird memcached process without a -p argument:", line
      continue
    port = line[port + 4 : line.index(" ", port + 5)]
    port = int(port)
    if port in DATASETS:
      print >>sys.stderr, "Host and port: %s %d" % (host, port)
      yield host,port
    else:
      print >>sys.stderr, "Unknown memached port:", port

def collect_stats(sock):
  """Sends the 'stats' command to the socket given in argument."""
  sock.send("stats\r\n")
  stats = sock.recv(2048)
  stats = [line.rstrip() for line in stats.split("\n")]
  assert stats[-1] == "", repr(stats)
  assert stats[-2] == "END", repr(stats)
  # Each line is of the form: STAT statname value
  stats = dict(line.split()[1:3] for line in stats[:-2])
  stats["time"] = int(stats["time"])
  return stats

def collect_stats_slabs(sock):
  """Sends the 'stats slabs' command to the socket given in argument."""
  sock.send("stats slabs\r\n")

  in_stats = ""
  end_time = time.time() + 1

  # The output from 'stats slabs' is long enough that we never get it all in
  # the first call to sock.recv.  This is a dumb loop that allows us to wait
  # a little bit for the data, but doesn't stall the collector forever.
  while time.time() < end_time:
    in_stats += sock.recv(65536)
    stats = [line.rstrip() for line in in_stats.split("\n")]
    if stats[-1] == "" and stats[-2] == "END":
      break
    time.sleep(0.1)

  assert stats[-1] == "", repr(stats)
  assert stats[-2] == "END", repr(stats)

  # prep the stats for slabs.  note the -4 because there are two lines
  # at the bottom we don't want: active_slabs and total_malloced.
  out_stats = {}
  slabs = dict(line.split()[1:3] for line in stats[:-4])
  for stat in slabs:
    slab_id, stat_name = stat.split(":")
    if slab_id not in out_stats:
      out_stats[slab_id] = {}
    out_stats[slab_id][stat_name] = slabs[stat]
  return out_stats

def main(args):
  """Collects and dumps stats from a memcache server."""
  sockets = {}  # Maps a dataset name to a socket connected its memcached.
  for host,port in find_memcached():
    dataset = DATASETS[port]
    sockets[dataset] = socket.socket()
    sockets[dataset].connect((host, port))
  if not sockets:
    return 13  # No memcached server running.

  stats = {}  # Maps a dataset name to a dict that maps a stats to a value.
  slabs = {}  # Same, but for slabs.

  def print_stat(stat, dataset):
    print ("memcache.%s %d %s dataset=%s"
           % (stat, stats[dataset]["time"], stats[dataset][stat], dataset))

  def print_stat_slab(stat, slab_id, dataset):
    # note we purloin 'time' from the stats call above ...
    print ("memcache.slab.%s %d %s chunksize=%s dataset=%s"
           % (stat, stats[dataset]["time"], slabs[dataset][slab_id][stat],
               slabs[dataset][slab_id]["chunk_size"], dataset))

  while True:
    for dataset, sock in sockets.iteritems():
      stats[dataset] = collect_stats(sock)

      # Print all the important stats first.
      for stat in IMPORTANT_STATS:
        print_stat(stat, dataset)

      for stat in stats[dataset]:
        if (stat not in IMPORTANT_STATS_SET      # Don't re-print them.
            and stat not in IGNORED_STATS_SET):  # Don't record those.
          print_stat(stat, dataset)

      # now do above, but for slabs
      slabs[dataset] = collect_stats_slabs(sock)

      for slab_id in slabs[dataset]:
        for stat in IMPORTANT_STATS_SLAB:
          print_stat_slab(stat, slab_id, dataset)

        for stat in slabs[dataset][slab_id]:
          if (stat not in IMPORTANT_STATS_SLAB_SET      # Don't re-print them.
              and stat not in IGNORED_STATS_SLAB_SET):  # Don't record those.
            print_stat_slab(stat, slab_id, dataset)

    sys.stdout.flush()
    time.sleep(COLLECTION_INTERVAL)


if __name__ == "__main__":
  sys.stdin.close()
  sys.exit(main(sys.argv))

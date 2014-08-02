import os
import re
import subprocess
import sys
import time
import select
from threading  import Thread
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # python 3.x
import pyinotify

QUEUE_FINISHED = object()

class TSDBMetricData:
    def __init__(self, metric, value, tags=[]):
        self.metric = metric
        self.value = value
        self.tags = tags
    def __str__(self):
        return "<metric: %s, value: %s, tags: %s>" % (self.metric, self.value, self.tags)


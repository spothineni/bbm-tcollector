import sys
from bbm import TSDBMetricData
from bbm.jmx import JMXPattern

def threading2tsdb(v):
    metric = "jvm.threads."
    attr = v.metric[14:-11].lower()
    if attr == "":
        attr = "current"
    elif attr == "totalstarted":
        attr = "total"
    metric = metric + attr
    value = v.value
    return TSDBMetricData(metric,value)

def memory2tsdb(v):
    # e.g. jmx.java.lang.Usage.used
    data = v.metric.split(".")

    metric = "jvm.memory." + data[4]
    value = v.value
    memtype = data[3][:-11]
    tags = [ "type=" + memtype ]

    return TSDBMetricData(metric,value, tags)

def fds2tsdb(v):
    metric = "jvm.filedescriptors."
    attr = v.metric[14:-19].lower()
    metric = metric + attr
    value = v.value
    return TSDBMetricData(metric,value)

def gc2tsdb(v):
    metric = "jvm.gc."
    tags = ["name=undefined"]
    for t in v.tags:
        if t.startswith("name="):
            tags = [ t ]
            break
    #jmx.java.lang.CollectionCount
    attr = v.metric[24:].lower()
    metric = metric + attr
    value = v.value
    return TSDBMetricData(metric,value,tags)

def mempool2tsdb(v):
    # e.g. jmx.java.lang.HeapMemoryUsage.init
    data = v.metric.split(".")

    metric = "jvm.mempool." + data[4]
    value = v.value
    tags  = ["name=unknown"]
    for t in v.tags:
        if t.startswith("name="):
            tags = [ t.translate(None," ") ]

    return TSDBMetricData(metric,value, tags)

jvm_collector = [
    JMXPattern("java.lang:type=Threading","^(PeakThread|DaemonThread|Thread|TotalStartedThread)Count$",threading2tsdb),
    JMXPattern("java.lang:type=MemoryPool", "^Usage($|\.)",mempool2tsdb),
    JMXPattern("java.lang:type=Memory$", "^(NonHeap|Heap)MemoryUsage",memory2tsdb),
    JMXPattern("java.lang:type=OperatingSystem", ".*FileDescriptorCount$",fds2tsdb),
    JMXPattern("java.lang:type=GarbageCollector", "^Collection",gc2tsdb),
    ]


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
    metric = "jvm.memory."
    # e.g. jmx.java.lang.HeapMemoryUsage
    memtype = v.metric[14:-11].lower()
    tags = [ "type=" + memtype ]
    newval =  TSDBMetricData(metric,"0", tags)

    # e.g. big horrid string thing
    valuesStr = v.value.split("{")[1].split("}")[0]
    values = map(lambda x: x.strip(" "), valuesStr.split(","))
    results = []
    for value in values:
      valtype, valval = value.split("=")
      results = results + [ TSDBMetricData(metric + valtype, valval, tags) ]
    return results

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
    metric = "jvm.memory.pool."
    tags = ["name=undefined"]
    for t in v.tags:
        if t.startswith("name="):
            tags = [ t.translate(None," ") ]
            break

    # e.g. big horrid string thing
    valuesStr = v.value.split("{")[1].split("}")[0]
    values = map(lambda x: x.strip(" "), valuesStr.split(","))
    results = []
    for value in values:
      valtype, valval = value.split("=")
      results = results + [ TSDBMetricData(metric + valtype, valval, tags) ]
    return results

jvm_collector = [
    JMXPattern("java.lang:type=Threading","^(PeakThread|DaemonThread|Thread|TotalStartedThread)Count$",threading2tsdb),
    JMXPattern("java.lang:type=Memory", "^(NonHeap|Heap)MemoryUsage$",memory2tsdb),
    JMXPattern("java.lang:type=OperatingSystem", ".*FileDescriptorCount$",fds2tsdb),
    JMXPattern("java.lang:type=GarbageCollector", "^Collection",gc2tsdb),
    JMXPattern("java.lang:type=MemoryPool", "^Usage$",mempool2tsdb),
     
    ]


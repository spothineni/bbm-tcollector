from bbm import TSDBMetricData
from bbm.jmx import JMXPattern

def c3p0conns2tsdb(v):
    metric = "c3p0.connections."
    attr = v.metric[27:-11].lower()
    if attr == "":
        attr = "total"
    metric = metric + attr
    value = v.value
    return TSDBMetricData(metric,value)

c3p0_collector = [
    JMXPattern("com.mchange.v2.c3p0:type=PooledDataSource", "^num.*Connections$", c3p0conns2tsdb)
    ]


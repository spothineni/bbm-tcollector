from bbm import TSDBMetricData
from bbm.jmx import JMXPattern

def accesses2tsdb(v):
    metric = "tomcat.accesses"
    webappName = v.metric[4:-13]
    value = v.value
    for t in v.tags:
        if t.startswith("name="):
            tags = ["webapp=" + webappName, "service=" + t[5:].strip("\"")]
            break
    return TSDBMetricData(metric,value,tags)

tomcat_collector = [
    JMXPattern("type=GlobalRequestProcessor", "^requestCount$", accesses2tsdb)
    ]

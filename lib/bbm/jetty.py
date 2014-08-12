from bbm import TSDBMetricData
from bbm.jmx import JMXPattern

def jetty2tsdb(v):
    v.metric = v.metric.translate(None,"\"")
    # We don't need the type tag, and need to remove some erroneous chars
    v.tags = [ x.translate(None,"\"") for x in v.tags if not x.startswith("type=") ]

    if v.metric == "jmx.org.eclipse.jetty.server.nio.Count":
        v.metric = "jetty.connections"
        # Remove the duration counter
        # Rename tag to state
        tags = []
        for t in v.tags:
            if t.endswith("-duration"):
                return[]
            if t.startswith("name="):
                tags = tags + [ "state=" + t[len("name="):] ]
            if t.startswith("scope="):
                tags = tags + [ "port=" + t[len("scope="):] ]
        v.tags = tags
        return v

    if v.metric == "jmx.org.eclipse.jetty.servlet.Count":
        v.metric = None
        tags = []
        for t in v.tags:
            if t.startswith("name=") and t.endswith("-responses"):
                v.metric = "jetty.reponses"
                m = t[len("name="):-1 * len("-responses") ]
                if m[0].isdigit():
                    tags = tags + [ "http_status=" + m ] 
            elif t.startswith("name=") and t.endswith("-requests"):
                v.metric = "jetty.requests"
                m = t[len("name="):-1 * len("-requests") ]
                if m.startswith("active"):
                    v.metric = v.metric + "." + m
                else:
                    tags = tags + [ "http_method=" + m.upper() ]
            else:
                tags = tags + [ t ]

        v.tags = tags
        if v.metric == None:
            return []
        else:
            return v


    if v.metric == "jmx.org.eclipse.jetty.util.thread.Value":
        v.metric = "jetty.threads"
        return v

    return []

jetty_collector = [
    JMXPattern(".*type=\"QueuedThreadPool.*-threads\"", "Value", jetty2tsdb),
    JMXPattern(".*type=\"ServletContextHandler\".*-requests\"", "Count", jetty2tsdb),
    JMXPattern(".*type=\"ServletContextHandler\".*-responses\"", "Count", jetty2tsdb),
    JMXPattern(".*type=\"BlockingChannelConnector\"", "Count", jetty2tsdb)
    ]

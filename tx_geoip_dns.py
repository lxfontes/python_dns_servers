# run with twistd -ny tx_geoip_dns.py
# gleicon 2011
# lxfontes 2012

from twisted.names import dns, server, client, cache
from twisted.application import service, internet
from twisted.internet import defer
from twisted.python import log
import txredisapi
import urllib2
import json

class RedisResolverBackend(client.Resolver):
    def __init__(self, redis, servers=None):
        self.redis = redis
        client.Resolver.__init__(self, servers=servers)
        self.ttl = 3600

    @defer.inlineCallbacks
    def locate(self, ip):
        parts = ip.split('.',4)
        ip = ".".join(reversed(parts))
        print("Looking for %s" % ip)
        location = yield self.redis.get("GEOIP:%s" % ip)
        if location:
            defer.returnValue(location)
        print("Looking up freegeoip: %s" % ip)
        #check with freegeoip
        try:
            body = urllib2.urlopen("http://freegeoip.net/json/%s" % ip).read()
            jobj = json.loads(body)
            location = "%s, %s, %s" % (jobj['city'], jobj['region_name'],
                               jobj['country_name'])
            yield self.redis.setex("GEOIP:%s" % ip, location, self.ttl)
            defer.returnValue(location)
        except:
            defer.returnValue(None)

    @defer.inlineCallbacks
    def _handle_ptr(self, name, timeout=None):
        #get rid of in-addr.arpa
        location = yield self.locate(name[:-13])
        if location:
            print("FOUND")
            txt = dns.RRHeader(name, dns.TXT, dns.IN, self.ttl, dns.Record_TXT(location))
            defer.returnValue([(txt,),(),()])

    def lookupPointer(self, name, timeout = None):
        return self._handle_ptr(name, timeout)


def create_application():
    rd = txredisapi.lazyConnectionPool()
    redisBackend = RedisResolverBackend(rd, servers=['8.8.8.8'])

    application = service.Application("txdnsredis")
    srv_collection = service.IServiceCollection(application)

    dnsFactory = server.DNSServerFactory(caches=[cache.CacheResolver()], clients=[redisBackend])

    internet.TCPServer(53, dnsFactory).setServiceParent(srv_collection)
    internet.UDPServer(53, dns.DNSDatagramProtocol(dnsFactory)).setServiceParent(srv_collection)
    return application

# .tac app
application = create_application()


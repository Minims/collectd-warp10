import collectd
import re
import copy
import sys, traceback
import urllib2

from Queue import Queue, Empty

class Warp10(object):
    def __init__(self):
        self.warp10_url = None
        self.warp10_token = None
        self.prefix = "prefix" # prefix
        self.tags_default = {}
        self.buffer_size = 1024
        self.queue = Queue()
        self.counter = 0

    def init(self):
        collectd.info("Starting write_warp10 plugin...")

    def config(self, conf):
        for node in conf.children:
            if node.key == 'url':
                self.warp10_url = node.values[0]
            elif node.key == 'token':
                self.warp10_token = node.values[0]
            elif node.key == 'prefix':
                self.prefix = node.values[0]
            elif node.key == 'tag':
                if len(node.values) == 2:
                    self.tags_default[node.values[0]] = node.values[1]
                else:
                    collectd.warning('warp10: Invalid tag config: %s' % node.values)
            elif node.key == 'buffer_size':
                self.buffer_size = int(node.values[0])
            else:
                collectd.warning('warp10: Unknown config key: %s.' % node.key)

    def flush(self, timeout=-1, plugins=[], identifiers=[]):
            self.postData(self.queue)

    def write(self, vl):
        datasets = collectd.get_dataset(vl.type)
        for ds, value in zip(datasets, vl.values):
            ds_name, ds_type, ds_min, ds_max = ds

            tags = copy.copy(self.tags_default)

            if vl.plugin:
                if vl.host is not None:
                    tags.update({ "host": vl.host })
                if vl.plugin_instance:
                    tags.update({ vl.plugin + "_instance": vl.plugin_instance })
                if vl.type is not None:
                    tags.update({ vl.plugin + "_type": vl.type})
                if vl.type_instance is not None:
                    tags.update({ vl.plugin + "_type_instance": vl.type_instance })
                if ds_name != "value":
                    tags.update({ vl.plugin + "_point": str(ds_name) })

            label = self.build_warp10_metric(self.prefix,vl.plugin,ds_type)
            collectd.debug("Label: %s" % label)
            if label is None:
                continue

            msg = '%d// %s{%s} %f' % (
                int(1000000*vl.time),  # Time is in microseconds
                label,
                ', '.join(['%s=%s' % (t, v) for t, v in tags.iteritems()]),
                value)
            self.queue.put(msg)
            if self.queue.qsize() >= self.buffer_size:
                collectd.debug("Queue is %d/%d" % (self.queue.qsize() ,self.buffer_size))
                self.postData(self.queue)

    def build_warp10_metric(self, *arr):
        return ".".join([re.sub(r'[^-_a-z0-9.]', '_', x.lower())
                         for x in arr if x])

    def postData(self,s):
        messages = []
        try:
            while True:
                messages.append(self.queue.get_nowait())
        except Empty:
            pass

        if len(messages) > 0:
                try:
                    warp10_headers =    {
                                            'X-Warp10-Token': self.warp10_token,
                                            'X-CityzenData-Token': self.warp10_token
                                        }
                    req = urllib2.Request(
                                            self.warp10_url,
                                            "\n".join(messages),
                                            warp10_headers
                                         )
                    resp = urllib2.urlopen(req)
                    collectd.debug("%d messages POST to Warp10" % len(messages) )
                    if resp.getcode() != 200:
                        raise Exception('%d %s' % (resp.getcode(),resp.read()))

                except Exception as e:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    e_str = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
                    collectd.error('warp10: Failed to post metrics: %s' % e_str)


WARP10 = Warp10()
collectd.register_config(WARP10.config)
collectd.register_init(WARP10.init)
collectd.register_flush(WARP10.flush)
collectd.register_write(WARP10.write)

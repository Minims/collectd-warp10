# collectd-warp10
Collectd Plugin for Warp10 Platform

# Configuration Example

 - Copy write_warp10.py in /usr/local/collectd/plugins/ for example.

 - Add this to your collectd.conf

```
<LoadPlugin "python">
    Globals true
</LoadPlugin>

<Plugin "python">
  ModulePath "/usr/local/collectd/plugins"
  LogTraces true
  Interactive false
  Import "write_warp10"
  <Module "write_warp10">
    tag node        "my_node"
    tag server_type "web
    token           "secret"
    url             "https://my_warp10_backend/api/v0/update"
  </Module>
</Plugin>
```

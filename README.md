# search2_tools
Collection of tools for managing search2 indexes



## Usage


### Config

Fetch the config from here: https://raw.githubusercontent.com/kbase/index_runner/develop/spec/config.yaml

Environment Variables
```
export WS_URL=https://ci.kbase.us/services/ws
export KB_ADMIN_TOKEN=xxxxxxxxxxxxxxxxxxxxx
export ELASTIC_URL=http://elasticsearch1:9500/
export ELASTIC_BASE=search2.default_search
export KAFKA_HOST=graph1
export ADMIN_TOPIC=cibackadmin
```


### Crawl

Crawl will scan workspaces and compare what is in the workspace to what is in elastic.  If RELOG is set
then it will write that to a file.

```
RELOG=1 crawl.py reindex.log 10000 20000
```

Even if RELOG is unset you still have to provide a log file name at the moment.

### Reindex

Reindex will read the log from a crawl and generate admin index events.

Send index nonexistant events.
```
reindex.py reindex.log
```

Send REINDEX events...
```
reindex.py -f reindex.log
```


## TODO

The crawl script could use some UI improvements



# Eventurtime: A Simple Event Collector

A sqlite backed log of events by UTC timestamp. 

It can collect:

* process return code, stdout
* HTTP GET/POST requests

During benchmarking and long term multiple process system evolution
we have a need to collect external events and high level markers.
This can easily be accomplished with `eventurtime` with a combination
of process executors and http based logging.

```
python -m eventurtime --database perf-run.db --port 9050
```

# Process Based Event Collector

In another terminal,

```
sqlite3 perf-run.db

sqlite> insert into job (time_delta_s,active,command) values (15,1,'redis-cli info');
sqlite> insert into job(time_delta_s,active,command) values (15,1,'netstat -ib');

-- wait for existing tasks to complete

select * from process_event;

```

# HTTP Based Event Collector

`GET` and `POST` requests work with any path at the port specified. The returned document 
is

```
content-type: text/plain

<timestamp of request>
```

```
curl -s http://0.0.0.0:9050/any/path/you/want
```

or

```
curl --data-binary "text" -s http://0.0.0.0:9050/also/takes/post/requests
```

Will log

```
sqlite> select * from http_event;
1470404430|GET|{'Host': '0.0.0.0:9050', 'User-Agent': 'curl/7.43.0', 'Accept': '*/*'}|/any/path/you/want|('127.0.0.1', 64771)|
```

# Usage

In your scripts and programs you can use `curl` to inject arbitrary events to
log. Like dropping a database, restarting a web server or configuring a proxy.
Stuff that is difficult to log along with memory usage and open files.

```bash

curl -s http://0.0.0.0:9050/restarting_database
service postgres restart
curl -s http://0.0.0.0:9050/restarting_haproxy
service haproxy restart

```





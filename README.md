# redis-slowlogs-streamer

Polls Redis Slow logs into parseable log files.

# Objective

Persist Redis SlowLogs on files so that they can be parsed and ingested using a third party tool for observability purposes. 

# Main Features

* Streamer allows configurable slowlog latency threshold  (defaults to 10ms)
* Consumer outputs  text files ton configurable locations
* log rotation supported
* Streamer should restore slowlogs default settings (threshold and size) before exiting

# Architecture

![architcture](./img/arch-2024-11-12-1710.png)

Multiple Streamers Consumers can be used in parallel to scale out the process to multiple databases and streams.

# How to use
< coming soon >


# Parsing Redis Slowlogs and ingest them to Elasticsearch

This can be easily done using  [the redis slow logs parser]([https://pages.github.com/](https://github.com/zumo64/redis-logs-parser)). 
You will be able to track command latency in real time !

# Example usage

### Start the slowlog streamer and connect to a Redis Enterprise database:

```
python slowlogs_streamer.py -c zu743.primary.cs.redislabs.com -h 172.31.43.246 -p 18817 -a redis -stream_port 6389 -threshold 6
```

### Start the slowlog streamer and connect to a Redis CE database:

```
python slowlogs_streamer.py -c zumo.redis.test.localhost -h localhost -p 6379 -stream_host localhost -stream_port 6389 -threshold 6
```

### Start the slowlog Consumer :
```
python slowlogs_consumer.py -h 127.0.0.1 -p 6389 -stream zumo.redis.test.localhost:6379 -root_dir /Users/zumo/dev/SupportPackages/Redis-CS
```

# redis-slowlogs-streamer

 Polls Redis Slow logs into parseable log files 


# Example usage

## Start the slowlog streamer and connect to a Redis Enterprise database:

```
python slowlogs_streamer.py -c zu743.primary.cs.redislabs.com -h 172.31.43.246 -p 18817 -a redis -stream_port 6389 -threshold 6
```

## Start the slowlog streamerand connect to a Redis CE database:

```
python slowlogs_streamer.py -c zumo.redis.test.localhost -h localhost -p 6379 -stream_host localhost -stream_port 6389 -threshold 6
```


## Start the slowlog Consumer:
```
python slowlogs_consumer.py -h 127.0.0.1 -p 6389 -stream zumo.redis.test.localhost:6379 -root_dir /Users/christianzumbiehl/dev/SupportPackages/Redis-CS
```

# Architecture

![architcture](./img/arch-2024-11-12-1710.png)


# Parsing Redis Slowlogs and ingest them to Elasticsearch

This can be easily done using  [the redis slow logs parser]([https://pages.github.com/](https://github.com/zumo64/redis-logs-parser)). 
You will be able to track command latency in real time !


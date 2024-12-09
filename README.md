# redis-slowlogs-streamer
 Polls Redis Slow logs into parseable log files 


# Example usage
python slowlogs_streamer.py -c zu743.primary.cs.redislabs.com -h 172.31.43.246 -p 18817 -a redis -stream_port 6389 -threshold 6

python slowlogs_consumer.py -h 127.0.0.1 -p 6389 -stream zumo.redis.test.localhost:6379 -root_dir /Users/christianzumbiehl/dev/SupportPackages/Redis-CS

from email.policy import default

import redis
import threading
import time
import argparse
import sys



import redis.commands.graph.commands


def custom_usage():
    """
    Custom usage/help message explaining each parameter.
    """
    usage = """
    Usage: python3 hotkeys.py -h <host> -p <port> [-l] [-t <time>] [-T <interval>] [-H | -help | help | ?]

    Parameters:
    -h <host>  : Host (FQDN) of the target Redis database (default: localhost).
    -p <port>  : Port of the target Redis database (default: 6379).
    -stream_host <host>  : Host (FQDN) of the Redis database used  to streamg Slowlogs  (default: localhost).
    -stream_port <port>  : Port of the Redis  database used  to stream Slowlogs (default: 6389).
    -stream_user <user>  : user of the Redis database used to stream Slowlogs
    -stream_password <password> : password of the Redis Streaming database
    -c : Cluster name or FQDN (defaults to hostname defined in -h) you can use if no DNS avail
    -t time : Time (s) to run the Streamer (default -1 (for ever))
    -threshold Slowlog threshold in micro seconds (default 10000): any commands slower than the value will be included in the slowlogs - 0 logs all commands 
     """
    print(usage)


def parse_arguments():
    """
    Parse command-line arguments and handle help options.
    """
    parser = argparse.ArgumentParser(add_help=False)

    # Mandatory parameters for host (-h) and port (-p)
    parser.add_argument('-h', default='localhost', type=str, help='Host (FQDN) of the Redis database (default: localhost)')
    parser.add_argument('-p', default=6379, type=int, help='Port of the Redis database (default: 6379)')
    parser.add_argument('-a', default=None, type=str, help='password')
    parser.add_argument('-u', default=None, type=str, help='user')
    parser.add_argument('-c', default=None, type=str, help='Cluster name or FQDN (defaults to hostname defined in -h) you can use if no DNS avail')

    parser.add_argument('-stream_host', default='localhost', type=str, help='Host (FQDN) of the Redis database (default: localhost)')
    parser.add_argument('-stream_port', default=6389, type=int, help='Port of the Redis Streaming database (default: 6389)')
    parser.add_argument('-stream_user', default=None, type=str, help='user of the Redis Streaming database')
    parser.add_argument('-stream_password', default=None, type=str, help='password of the Redis Streaming database')
    parser.add_argument('-max_stream_size', default=-1, type=str, help='Maximum nummber of elements in the stream')


    # Optional flags and parameters
    parser.add_argument('-threshold', type=int, default=10000, help='Slowlog threshold in micro seconds (default 10000): any commands slower than the value will be included in the slowlogs - use 20 as a minimum')
    parser.add_argument('-t', type=int, default=-1, help='Time in seconds  to operate the script before terminating (default: -1 for "run for ever")')
    parser.add_argument('-T', type=float, default=10, help='Sleep interval in milliseconds between consecutive loops (default: 10 ms)')
    parser.add_argument('-ignore', type=str, default='SLOWLOG,SPING',help='Ignore some commands in the stream')

    # Check if custom help is requested by handling multiple help options
    parser.add_argument('-H', action='store_true', help='Show help')
    parser.add_argument('-help', action='store_true', help='Show help')
    parser.add_argument('help_arg', nargs='?', default=None, help='Handle ? and help')

    args = parser.parse_args()


    # Display custom help if any help-related flag or argument is detected
    if args.H or args.help or args.help_arg in ['help', '?']:
        custom_usage()
        sys.exit(0)

    return args


# TODO implement the maxlen argument
def addlog(r,key,log):

    stime = str(log.get('start_time'))+"-*"
    logid = log.get('id')
    #print(f" ------> added in log stream element : key {key} -  logid {logid}  id  {stime} command {log.get('command')}")
    r.xadd(key , log, str(stime))
    return logid


def starts_with_token_ignore_case(s, tokens):
    # Convert the input string to lower case for case insensitive comparison
    s = s.lower()
    # Check if the string starts with any of the tokens (also in lower case)
    return any(s.startswith(token.lower()) for token in tokens)

def poll_slowlogs(r, ro, stop_event, sleep_interval,key,black_listed_commands):

    # Keep track of the most recent slow log sent
    top_of_list_id = -1
    countrecords = 0

    while not stop_event.is_set():

        # Get all slowlogs in buffer
        sl = r.slowlog_get(-1)

        # reverse the order of slowlogs
        # to send  most recent log last (top of the Stream)
        for i, log in enumerate(reversed(sl)):

            current_id = log.get('id')

            # ignore entries until reaching top of list (new slowlogs)
            if current_id <= top_of_list_id :
                continue


            # ignore  commands that are not relevant  in the stream (black listed)
            command = str(log.get('command'), 'utf-8')
            if starts_with_token_ignore_case(command , black_listed_commands):
                continue

            top_of_list_id = current_id

            try:
                addlog(ro,key, log)

            #except redis.exceptions.RedisError as e:
                #print(e)

            finally:
                continue

        time.sleep(sleep_interval)



def main():
    # Parse command-line arguments
    args = parse_arguments()


    try:
        # Connect to the Redis server
        rprod = redis.Redis(host=args.h, port=args.p, username=args.u, password=args.a)
        print(f"Polling slowlogs from Redis database {args.h}:{str(args.p)}")
        rstream = redis.StrictRedis(host=args.stream_host, port=args.stream_port, decode_responses=True)
        print(f"Streaming Slowlogs to host {args.stream_host}:{str(args.stream_port)}")



        # start logging everything
        rprod.config_set("slowlog-max-len",1024)
        # slowlog-log-slower-than expects microseconds
        rprod.slowlog_reset()
        print(f"using Threshold : {args.threshold} micro seconds")

        rprod.config_set("slowlog-log-slower-than", args.threshold)
        # stream name is <cluster FQDN>:<DB port>
        # defaults to host name
        if args.c != None:
            key = args.c + ':' + str(args.p)
        else:
            key = args.h +':'+ str(args.p)

        black_list_tokens = []
        if args.ignore != None:
            black_list_tokens = [token.strip() for token in args.ignore.split(',')]

        print(f"using stream name: {key}")
        # Start a separate thread to listen for event space notifications
        stop_event = threading.Event()
        slowlog_poller_thread = threading.Thread(target=poll_slowlogs, args=(rprod, rstream,  stop_event, 20, key, black_list_tokens ))
        slowlog_poller_thread.start()
        # Sleep for ever is default
        if args.t == -1:
            while True:
                time.sleep(1)
        else:
            time.sleep(args.t)

        # Signal the listener thread to stop
        stop_event.set()
        slowlog_poller_thread.join()
        print("Tearing down and Exiting...")

        # restore slowlogs default
        rprod.config_set("slowlog-max-len", "128")
        # put back default to 10ms
        rprod.config_set("slowlog-log-slower-than", 10000)


    # Put back Slow logs defaults
    except KeyboardInterrupt:
        rprod.config_set("slowlog-max-len", "128")
        rprod.config_set("slowlog-log-slower-than", 10000)
        stop_event.set()
        slowlog_poller_thread.join()
        print("Restored slowlog defaults and")
        print("Exiting...")

if __name__ == "__main__":
    main()

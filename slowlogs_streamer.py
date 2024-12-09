from email.policy import default

import redis
import threading
import time
import argparse


import redis.commands.graph.commands


def custom_usage():
    """
    Custom usage/help message explaining each parameter.
    """
    usage = """
    Usage: python3 hotkeys.py -h <host> -p <port> [-l] [-t <time>] [-T <interval>] [-H | -help | help | ?]

    Parameters:
    -h <host>  : Host (FQDN) of the Redis database (default: localhost).
    -p <port>  : Port of the Redis database (default: 6379).
    -hstream <host>  : Host (FQDN) of the Redis Streaming database (default: localhost).
    -pstream <port>  : Port of the Redis Streaming database (default: 6389).
    -astream <port>  : user:password
    -c : Cluster alias or FQDB (defaults to hostname)
    -t time : Time to run the Streamer
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
    parser.add_argument('-c', default=None, type=str, help='Cluster alias or FQDB (defaults to hostname)')

    parser.add_argument('-hstream', default='localhost', type=str, help='Host (FQDN) of the Redis database (default: localhost)')
    parser.add_argument('-pstream', default=6389, type=int, help='Port of the Redis database (default: 6389)')
    parser.add_argument('-astream', default='redis', type=str, help='user:password')

    # Optional flags and parameters
    parser.add_argument('-l', action='store_true', help='List the current content of hotkeys and exit')
    parser.add_argument('-t', type=int, default=10, help='Time to operate the script before terminating (default: 10 seconds, range: 1-100)')
    parser.add_argument('-T', type=float, default=10, help='Sleep interval in milliseconds between consecutive loops (default: 10 ms)')

    args = parser.parse_args()

    return args


def addlog(r,key,log):

    stime = str(log.get('start_time'))+"-*"
    logid = log.get('id')
    command = log.get('command')
    r.xadd(key , log, str(stime))
    #print(f" ------> added in log stream element : key {key} -  logid {logid}  id  {stime} command {command} ")
    return logid



def poll_slowlogs(r, ro, stop_event, sleep_interval,key):

    topOfListId = -1
    countrecords = 0

    while not stop_event.is_set():

        # Get all slowlogs in buffer
        # -1 generates an Exception with RS
        sl = r.slowlog_get(-1)

        # reverse the order to send  most recent log last (top of the Stream)
        for i, log in enumerate(reversed(sl)):

            currentId = log.get('id')

            # ignore entry until reaching new slowlogs
            if currentId <= topOfListId :
                continue

            # ignore some commands that are useless in the stream
            command = str(log.get('command'), 'utf-8')
            if command.startswith('SLOWLOG') or command.startswith('SPING'):
                continue

            if i == len(sl)-1:
                topOfListId = currentId

            try:
                addlog(ro,key, log)
                countrecords = countrecords+1

            finally:
                continue

        #print(f"added {countrecords} log events")

        time.sleep(sleep_interval)


def main():
    # Parse command-line arguments
    args = parse_arguments()

    try:
        # Connect to the Redis server
        rprod = redis.Redis(host=args.h, port=args.p, username=args.u, password=args.a)
        print(f"Polling slowlogs from Redis database {args.h}:{str(args.p)}")
        rstream = redis.StrictRedis(host=args.hstream, port=args.pstream, decode_responses=True)



        # start logging everything
        rprod.config_set("slowlog-max-len",1024)
        rprod.config_set("slowlog-log-slower-than", 0)

        # stream name is <cluster FQDN>_<DB port>
        # defaults to host name
        if args.c != None:
            key = args.c + ':' + str(args.p)
        else:
            key = args.h +':'+ str(args.p)

        print(f"Streaming logs to stream {key} on Redis database {args.hstream}:{str(args.pstream)}")
        # Start a separate thread to listen for event space notifications
        stop_event = threading.Event()
        slowlog_poller_thread = threading.Thread(target=poll_slowlogs, args=(rprod, rstream,  stop_event, 5, key ))
        slowlog_poller_thread.start()
        time.sleep(args.t)

        # Signal the listener thread to stop
        stop_event.set()
        slowlog_poller_thread.join()
        print("Tearing down and Exiting...")

        # restore slowlogs default
        rprod.config_set("slowlog-max-len", "128")
        rprod.config_set("slowlog-log-slower-than", 10000)



    except KeyboardInterrupt:
        rprod.config_set("slowlog-max-len", "128")
        rprod.config_set("slowlog-log-slower-than", 10000)
        stop_event.set()
        slowlog_poller_thread.join()
        print("Exiting...")

if __name__ == "__main__":
    main()
import redis
from datetime import datetime
import os
import argparse
from datetime import datetime, timezone
import threading
from redis.exceptions import ResponseError


output_file_prefix = 'slowlog'
max_size = 500000

# testing with
#-h 127.0.0.1
# -p 6389
#-stream db_6379
#-root_dir /Users/christianzumbiehl/dev/SupportPackages/Redis-CS
# -cluster standalone_DB_1
def parse_arguments():
    """
    Parse command-line arguments and handle help options.
    """
    parser = argparse.ArgumentParser(add_help=False)

    # Mandatory parameters for host (-h) and port (-p)
    parser.add_argument('-h', default='localhost', type=str, help='Host (FQDN) of the Redis database used for streaming (default: localhost)')
    parser.add_argument('-p', default=6379, type=int, help='Port of the Redis database (default: 6379)')
    parser.add_argument('-a', default='redis', type=str, help='user:password')
    parser.add_argument('-z', action="store_true", help='If specified reads from the beginning of the stream')
    parser.add_argument('-ts', action="store_true", help='creates a TS for each command from the Stream')
    parser.add_argument('-outfile', action="store_true", help='dumps Slow Log files in the specified root_dir')
    parser.add_argument('-stream', default="localhost:6379",  type=str, help='The name of the stream to consume logs from')
    parser.add_argument('-root_dir', default="/tmp/slowlogs", type=str, help='Root Folder tout output slowlogs Files to ')


    args = parser.parse_args()

    return args




# Function to consume and append messages to a file
def consume_stream(redis,folder_path,stream_name,fromBeginning, createTs, createFiles):

    current_time = datetime.now()
    formatted_time = current_time.strftime("%d.%m.%y-%H.%M.%S")
    output_file = output_file_prefix +"-"+formatted_time

    log_counter = 1
    file_path = os.path.join(folder_path, output_file+"-"+str(log_counter)+".log")
    print(f"Writing to File {file_path} ")

    if fromBeginning:
        lastid = '0'
        block = None
        print(f"Consuming all events in stream")

    else:
        lastid = '$'
        block = 5000
        print(f"Waiting for new Slow log events")

    print(f"Start consuming ...")
    while True:

        # Read messages from the stream with BLOCK to wait for new messages
        messages = redis.xread( streams= {stream_name : lastid},block=5000, count=50,)


        if createFiles:
            f = open(file_path, 'a')

        if messages:
            for stream, msg_list in messages:
                for message_id, message in msg_list:
                    # Parse message
                    startTime = int(message.get('start_time'))
                    #iso8601_timestamp = datetime.utcfromtimestamp(startTime).isoformat() + 'Z'
                    iso8601_timestamp = datetime.fromtimestamp(startTime, tz=timezone.utc).isoformat() + 'Z'
                    duration = float(message.get('duration')) / 1000
                    command_str = message.get('command')
                    command_parts = command_str.split()
                    # Initialize an empty string to store the result
                    output_command = ""
                    # Loop through the array and concatenate each string
                    count = 1
                    for s in command_parts:
                        output_command += "b'"+ s + "'"

                        # if we are in TS append mode , update the TS  with the command
                        if count == 1 and createTs:
                            labelsDict = {
                                "command": s,
                                "type":"slowlogs",
                                "series":stream_name
                            }

                            # log duration in miliseconds
                            redis.ts().add(stream_name+":"+s ,startTime  * 1000 ,duration * 1000,labels=labelsDict,duplicate_policy="max")

                        if count == len(command_parts) or not createFiles:
                            break
                        else:
                            output_command = output_command  + ", "
                            count = count + 1
                    if createFiles:
                        f.write(f"{message.get('id')} {iso8601_timestamp} {duration}   [{output_command}]\n")
                        #print(f"{message.get('id')} {iso8601_timestamp} {duration}   [{output_command}]")

                    lastid = message_id


        try:
            if createFiles:
                file_size = os.path.getsize(file_path)
                # New Log File every Milion bytes
                if file_size >= max_size:
                    f.close()
                    log_counter = log_counter+1
                    #file_path = os.path.join(folder_path, "slowlog-" + str(log_counter) + ".log")
                    file_path = os.path.join(folder_path,  output_file+"-"+str(log_counter)+ ".log")
                    print(f"Writing to File {file_path} ")

        finally:
            continue


def main():
    args = parse_arguments()

    # Expecting  stream name=  <clusterFQDN>:port
    cluster_and_db = args.stream.split(':')
    folder_path = os.path.join( args.root_dir,  cluster_and_db[0] , cluster_and_db[1])
    if args.outfile:
        os.makedirs(folder_path, exist_ok=True)

    # Connect to Redis
    r = redis.StrictRedis(host=args.h, port=args.p, decode_responses=True)

    print(f"Connected to Redis ...")

    try:
        stop_event = threading.Event()
        slowlog_consumer_thread = threading.Thread(target=consume_stream, args=(r,folder_path,args.stream,args.z,args.ts,args.outfile))
        slowlog_consumer_thread.start()
        #consume_stream(r,folder_path,args.stream,args.z)

    except KeyboardInterrupt:
        print("Exiting...")
        stop_event.set()
        slowlog_consumer_thread.join()
        


if __name__ == "__main__":
    main()
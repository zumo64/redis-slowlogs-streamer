import redis
from datetime import datetime
import os
import argparse
from datetime import datetime


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
    parser.add_argument('-z', default=False, type=bool, help='Read from the beggining (True) of the stream or from the end (default = False)')


    parser.add_argument('-stream', type=str, help='The name of the stream to consume logs from')
    parser.add_argument('-root_dir', default="/tmp/slowlogs", type=str, help='Root Folder tout output slowlogs Files to ')


    args = parser.parse_args()

    return args




# Function to consume and append messages to a file
def consume_stream(redis,folder_path,stream_name,fromBeginning):

    current_time = datetime.now()
    formatted_time = current_time.strftime("%d.%m.%y-%H.%M.%S")
    output_file = output_file_prefix +"-"+formatted_time

    log_counter = 1
    file_path = os.path.join(folder_path, output_file+"-"+str(log_counter)+".log")
    print(f"Writing to File {file_path} ")

    if fromBeginning:
        lastid = '0'
        block = None
        print(f"Streaming from zero")

    else:
        lastid = '$'
        block = 5000
        print("Waiting for new Slow log events")

    while True:

        # Read messages from the stream with BLOCK to wait for new messages
        messages = redis.xread( streams= {stream_name : lastid},block=5000, count=50)

        if messages:
            with (open(file_path, 'a') as f):
                for stream, msg_list in messages:
                    for message_id, message in msg_list:
                        iso8601_timestamp = datetime.utcfromtimestamp(int(message.get('start_time'))).isoformat() + 'Z'
                        duration = float(message.get('duration')) / 1000
                        command_str = message.get('command')
                        command_parts = command_str.split()
                        # Initialize an empty string to store the result
                        output_command = ""
                        # Loop through the array and concatenate each string
                        count = 1
                        for s in command_parts:
                            output_command += "b'"+ s + "'"

                            if count == len(command_parts):
                                break
                            else:
                                output_command = output_command  + ", "
                                count = count + 1

                        f.write(f"{message.get('id')} {iso8601_timestamp} {duration}   [{output_command}]\n")
                        #print(f"{message.get('id')} {iso8601_timestamp} {duration}   [{output_command}]")
                        lastid = message_id


        # Optional delay to control polling rate (not necessary with BLOCK)
        #time.sleep(1)
        try:
            file_size = os.path.getsize(file_path)
            # New Log File every Milion bytes
            if file_size >= max_size:
                log_counter = log_counter+1
                file_path = os.path.join(folder_path, "slowlog." + str(log_counter) + ".log")
                print(f"Writing to File {file_path} ")

        finally:
            continue


def main():
    args = parse_arguments()

    # Expecting  stream name=  <clusterFQDN>:port
    cluster_and_db = args.stream.split(':')
    folder_path = os.path.join( args.root_dir,  cluster_and_db[0] , cluster_and_db[1])
    os.makedirs(folder_path, exist_ok=True)

    # Connect to Redis
    r = redis.StrictRedis(host=args.h, port=args.p, decode_responses=True)

    print(f"Connected to Redis ..")
    consume_stream(r,folder_path,args.stream,args.z)

if __name__ == "__main__":
    main()
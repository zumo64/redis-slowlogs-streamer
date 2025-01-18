# Use the official Python base image
FROM python:3.9-slim AS logs-consumer

# Set the working directory in the container
WORKDIR /app

# Copy the requirements.txt file into the container
COPY requirements.txt /app/

# Install the Python packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code into the container
COPY slowlogs_consumer.py /app/

# Define the command to run your application
ENTRYPOINT ["python", "slowlogs_consumer.py"]


# Use the official Python base image
FROM python:3.9-slim AS logs-streamer

# Set the working directory in the container
WORKDIR /app

# Copy the requirements.txt file into the container
COPY requirements.txt /app/

# Install the Python packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code into the container
COPY slowlogs_streamer.py /app/

# Define the command to run your application
ENTRYPOINT [ "python", "slowlogs_streamer.py"]


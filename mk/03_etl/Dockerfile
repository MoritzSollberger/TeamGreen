# Use an official Python runtime as a parent image
FROM python:3.6-slim

# Set the working directory to /src
WORKDIR /src

# Copy the requirements file into the container at /src
# (a text files with all the libraries you want to install)
COPY requirements.txt /src

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Run src.py when the container launches
# CMD ["python", "twitter_streamer.py"]

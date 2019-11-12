# Use an official Python runtime as a parent image
FROM python:3.6-slim

# Set the working directory to /app
WORKDIR /app

# Copy the requirements file into the container at /app
# (a text files with all the libraries you want to install)
#requires re-building each time you update these files in local.
#better to just add it as a volumes shared file.

#ADD get_tweets_aj.py /app #no longer required because already loaded into /code virtual folder
#ADD requirements.txt /app #no longer required because already loaded into /code virtual folder
COPY requirements.txt /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Run app.py when the container launches
CMD ["python", "twitter_streamer.py"]

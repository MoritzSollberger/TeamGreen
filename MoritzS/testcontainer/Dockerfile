# Use an official Python runtime as a parent image
FROM python:3.6-slim

# Set the working directory to /src
WORKDIR /src

# Copy the requirements file into the container at /src
ADD requirements.txt /src
Add src/test_app.py /src

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Run app.py when the container launches
CMD ["python", "test_app.py"]

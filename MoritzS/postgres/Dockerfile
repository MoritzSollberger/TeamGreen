# Use an official Python runtime as a parent image
FROM postgres

#COPY init.sql /docker-entrypoint-initdb.d/

# Set the working directory to /app
WORKDIR /src

# Copy the requirements file into the container at /app
# (a text files with all the libraries you want to install)
COPY . /src


# Install any needed packages specified in requirements.txt
# RUN pip install --trusted-host pypi.python.org -r requirements.txt

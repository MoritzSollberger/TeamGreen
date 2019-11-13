# Use apache airflow as a parent image
FROM apache/airflow

# Set the working directory to /app
WORKDIR /app

COPY requirements.txt /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Run app.py when the container launches
CMD ["python", "etl_process.py"]

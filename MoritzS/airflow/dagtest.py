from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.contrib.hooks.mongo_hook import MongoHook
! pip install pymongo


# from pymongo import MongoClient
# CLIENT = MongoClient('localhost', 27017)
# db = CLIENT.twitter_data

# hook = MongoHook(conn_type = mongo)


dag = DAG(
	dag_id = 'test_dag',
	start_date = datetime(2019, 11, 13),
	schedule_interval = '* * * * *')

# def pull_latest_tweets():
# 	for tweet in db.tweets.find()

def print_hello():
	return "hello!"

print_hello = PythonOperator(
	task_id = 'print_hello',
	#python_callable param points to the function you want to run
	python_callable = print_hello,
	#dag param points to the DAG that this task is a part of
	dag = dag)

#Assign the order of the tasks in our DAG
print_hello # >> next_task

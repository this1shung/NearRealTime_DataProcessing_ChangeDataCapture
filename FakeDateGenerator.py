from cassandra.cluster import Cluster
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import  Model
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine import connection
from cassandra.query import dict_factory
from datetime import datetime, timedelta
import time
import cassandra
import random
import uuid
import math
import pandas as pd
pd.set_option("display.max_rows", None, "display.max_columns", None)
import datetime
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import mysql.connector

host = 'localhost'
port = '3306'
db_name = 'storagedb'
user = 'root'
password = '1234'
url = 'jdbc:mysql://' + host + ':' + port + '/' + db_name
driver = "com.mysql.cj.jdbc.Driver"

keyspace = 'mykeyspace'
cassandra_login = 'cassandra'
cassandra_password = 'cassandra'
cluster = Cluster()
session = cluster.connect(keyspace)

def get_data_from_job(user,password,host,database):
    cnx = mysql.connector.connect(user=user, password=password,
                                         host=host,
                                      database=database)
    query = """select id as job_id,campaign_id , group_id , company_id from job"""
    mysql_data = pd.read_sql(query,cnx)
    return mysql_data

def get_data_from_publisher(user,password,host,database):
    cnx = mysql.connector.connect(user=user, password=password,
                                         host=host,
                                      database=database)
    query = """select distinct(id) as publisher_id from master_publisher"""
    mysql_data = pd.read_sql(query,cnx)
    return mysql_data


def generating_dummy_data(n_records,session,user,password,host,db_name):
    publisher = get_data_from_publisher(user,password,host,db_name)
    publisher = publisher['publisher_id'].to_list()
    jobs_data = get_data_from_job(user,password,host,db_name)
    job_list = jobs_data['job_id'].to_list()
    campaign_list = jobs_data['campaign_id'].to_list()
    company_list = jobs_data['company_id'].to_list()
    group_list = jobs_data[jobs_data['group_id'].notnull()]['group_id'].astype(int).to_list()
    i = 0 
    fake_records = n_records
    while i <= fake_records:
        create_time = str(cassandra.util.uuid_from_time(datetime.datetime.now()))
        bid = random.randint(0,1)
        interact = ['click','conversion','qualified','unqualified']
        custom_track = random.choices(interact,weights=(70,10,10,10))[0]
        job_id = random.choice(job_list)
        publisher_id = random.choice(publisher)
        group_id = random.choice(group_list)
        campaign_id = random.choice(campaign_list)
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = """ INSERT INTO tracking (create_time,bid,campaign_id,custom_track,group_id,job_id,publisher_id,ts) VALUES ({},{},{},'{}',{},{},{},'{}')""".format(create_time,bid,campaign_id,custom_track,group_id,job_id,publisher_id,ts)
        print(sql)
        session.execute(sql)
        i+=1 
    return print("Data Generated Successfully")

status = "ON"
while status == "ON":
    generating_dummy_data(n_records = random.randint(1,2),session = session , user = user , password = password , host = host , db_name = db_name )
    time.sleep(60)


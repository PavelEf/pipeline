#!/usr/bin/env python
# coding: utf-8

# In[23]:


import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

# from pyspark.sql.types import StructType, StructField, StringType


# In[24]:


def create_keyspace(session):
    session.execute('''
        create KEYSPACE IF NOT EXIST spark_stream
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    ''')
    
    print('create_keyspace is OK')


# In[25]:


def create_table(session):
    session.execute('''
        CREATE TABLE IF NOT EXIST spark_steams.users (
            user_id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT
        )
    ''')
    
    print('create_table is OK')


# In[26]:


def data_to_cassandra(session, **kwargs):
    print('start insert data')
    
    user_id = kwargs.get('user_id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    
    try:
        session.execute('''
            INSERT INTO spark_steams.users(user_id, first_name, last_name, gender)
            VALUES (%s, %s, %s, %s)
        ''',
            (user_id, first_name, last_name, gender)
                       )
        
        logging.info(f'Data inserted for {user_id}')
    except Exception as e:
        logging.error(f'Could not insert data {e}')


# In[27]:


def spark_connection():
    spark = None
    
    try:
        spark = SparkSession \
            .builder \
            .appName('Spark Streaming') \
            .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.13:3.5.1', 'org.apache.spark:spark-sql-kafka-0-10_2.13:jar:3.5.1') \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel('Error')
        logging.info('Succes')
    except Exception as e:
        logging.error('Can not create spark session: {e}')
    
    return spark


# In[28]:


def kafka_connection(spark_conn): # https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    df = None # we are connecting to spark
    
    try:
        df = spark \
            .readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .option('endingOffsets', 'latest') \
            .load()
        
        logging.info('df create is OK')
    except Exception as e:
        logging.warning(f'df is BAD {e}')
    
    return df


# In[29]:


def cassandra_connection():
    cassandra_session = None
    
    try:
        cluster = Cluster(['localhost'])

        cassandra_session = cluster.connect()
        
        return cassandra_session
    
    except Exception as e:
        logging.error('Can not connect to cassandra {e}')
        
    return None


# In[30]:


def select_df_from_kafka(spark_df):
    schema = StructType([
        StructField('id', StringType(), False),
        StructField('first_name', StringType(), False),
        StructField('last_name', StringType(), False),
        StructField('gender', StringType(), False)
    ])
    
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


# In[31]:


if __name__ == '__main__':
    spark_conn = spark_connection()
    
    if spark_conn is not None:
        spark_df = kafka_connection(spark_conn)
        select_df = select_df_from_kafka
        session = cassandra_connection(spark_df)
        
        if session is not None:
            create_keyspace(session)
            create_table(session)
#             data_to_cassandra(session)
            stream = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'users')
                               .start())

            stream.awaitTermination()


# In[ ]:





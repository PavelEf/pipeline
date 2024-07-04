#!/usr/bin/env python
# coding: utf-8

# In[36]:


import requests
import json
import time
import logging


# In[51]:


from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


# In[38]:


from kafka import KafkaProducer


# In[39]:


def get_data(): # load data from API
    data = requests.get('https://randomuser.me/api/')
    data = data.json()
    data = data['results'][0]
#     print(json.dumps(data, indent=3))
#     print(data)
    return data


# In[40]:


def format_data(data): # делаем словарь с нужной нам структурой и названиями
    d = {}
    
    d['first_name'] = data['name']['first']
    d['last_name'] = data['name']['last']
    d['gender'] = data['gender']
#     d['adress']
#     postcode
#     email
#     username
#     dob
#     reg_date
#     phone
#     picture
    
    return d


# In[41]:


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


# In[44]:


def stream_data(): # setting Kafka
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        max_block_ms=5000
#         value_serializer=json_serializer
    )
    
    curr_time = time.time()
    
    while True:
        if curr_time + 60 < time.time():
            break
        try:
            res = get_data()
            res = format_data(res)
#     print(res)
#     print(json.dumps(res, indent=3))
    
            producer.send('users_created', json_serializer(res))# value=message)
#     producer.send('users_created', value=res)
        except Exception as e:
            logging.error(f'Error now: {e}')
            continue
    
#     producer.flush()
#     producer.close()


# In[46]:


# stream_data()


# In[52]:


default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 7, 4, 13, 10)
}


# In[54]:


with DAG('user_automation',
         default_args=default_args,
         schedule='*/5 * * * *',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )


# In[15]:


# get_data()


# In[16]:


# pip install kafka-python
# pip install confluent-kafka


# In[ ]:





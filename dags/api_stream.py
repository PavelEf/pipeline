#!/usr/bin/env python
# coding: utf-8

# In[14]:


import requests
import json
import time


# In[8]:


# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python import PythonOperator


# In[13]:


from kafka import KafkaProducer


# In[9]:


def get_data(): # load data from API
    data = requests.get('https://randomuser.me/api/')
    data = data.json()
    data = data['results'][0]
#     print(json.dumps(data, indent=3))
#     print(data)
    return data


# In[10]:


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


# In[21]:


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


# In[22]:


def stream_data(): # setting Kafka
    res = get_data()
    res = format_data(res)
#     print(res)
#     print(json.dumps(res, indent=3))

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092']
#         max_block_ms=5000
#         value_serializer=json_serializer
    )
    
    producer.send('users_created', json_serializer(res))# value=message)


# In[25]:


stream_data()


# In[ ]:





# In[15]:


# get_data()


# In[16]:


# pip install kafka-python
# pip install confluent-kafka


# In[ ]:





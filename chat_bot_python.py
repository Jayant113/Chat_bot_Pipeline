#!/usr/bin/env python
# coding: utf-8

# In[118]:


import pyspark
from google.cloud import bigquery


# In[119]:


import pandas as pd


# In[120]:


from pyspark.sql import SparkSession
spark = SparkSession.builder   .appName('1.1. BigQuery Storage & Spark DataFrames - Python')  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest.jar')   .getOrCreate()


# In[121]:


from pyspark.sql import SparkSession


# In[122]:


import inspect
from textwrap import dedent


# In[123]:


from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


# In[124]:


import datetime
import time


# In[125]:


from datetime import datetime
from pyspark.sql.functions import unix_timestamp


# In[126]:


import inspect
from textwrap import dedent


# In[127]:


df = pd.read_json("gs://main_chat_file/main_chatfile.json")


# In[128]:


df_new_chat = df[['insertId', 'trace' , 'timestamp' ]]


# In[129]:


df_new_chat


# In[130]:


df_new_chat.dtypes


# In[131]:


from google.cloud import bigquery
import pandas
client = bigquery.Client()
table_id = 'chat_bot.chat4'
job = client.load_table_from_dataframe(
    df_new_chat, table_id
)


# In[70]:


# from pyspark.sql import functions as f
# import numpy as np


# In[36]:


# df_new_chat.to_gbq(destination_table='chat_bot.chat',
#                   project_id='celestial-torus-342506',
#                   if_exists='append')
    


# In[ ]:





# In[ ]:





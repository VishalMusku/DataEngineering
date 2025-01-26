import requests
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
import time 
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta

def get_data():
    res=requests.get('https://randomuser.me/api/')

    return (res.json()['results'][0])


def format_data(res):

    data={}
    location=res['location']
    data['first_name']=res['name']['first']
    data['last_name']=res['name']['last']
    data['gender']=res['gender']
    data['address']=str(res['location']['city']+" "+res['location']['state']+" "+res['location']['country'])
    data['postcode']=location['postcode']
    data['email']=res['email']
    data['username']=res['login']['username']
    data['dob']=res['dob']['date']
    data['registered_date']=res['registered']['date']
    data['phone']=res['phone']
    data['picture']=res['picture']['medium']
        
    return data


def stream_data():
 
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],  
        value_serializer=lambda v: json.dumps(v).encode('utf-8') 
    )

    curr_time=time.time()
    
    while True:
        
        if time.time() > curr_time+60:
            break
        
        try:
            res = get_data()
            data = format_data(res)
            
            producer.send('users_created', value=data)
            producer.flush()
                
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue

   
default_args = {
    'owner': 'airscholar',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'user_automation',
    default_args=default_args,
    description='A DAG to stream user data to Kafka',
    schedule='@daily',  
    start_date=datetime(2024, 12, 31, 18, 47),
    catchup=False
) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )



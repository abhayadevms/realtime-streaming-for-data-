from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
print("hello")
import json
import requests
from kafka import KafkaProducer



default_args ={
    'owner':'airdev',
    'start':datetime(2024,5,9, 10, 00)


}
def get_data(path='https://randomuser.me/api/'):
    res = requests.get(url=path)
    res=res.json()
    return res['results'][0]

def format_data(res):
    data = {}
    data['name'] = res["name"]['first'] +' '+ res["name"]['last']
    location = res["location"]
    data['address'] = f'{location["street"]["number"]},{location["street"]["name"]},\
    {location["city"]},\
    {location["state"]},\
    {location["country"]},\
    {location["postcode"]},'
    data['email'] = res["email"]
    data["username"] = res["login"]["username"]
    data["password"] = res["login"]["password"]
    data["dob"] = res['dob']["date"]
    data["age"] = res["dob"]["age"]
    data["registered"] = res["registered"]["date"]
    data["phone"] = res["phone"]
    data["picture"] = res[ "picture"]["thumbnail"]
    
    return data



def stream_data():
    res = get_data()
    data = format_data(res)
    #print(json.dumps(data, indent=3))
    producer = KafkaProducer(bootstrap_servers='localhost:9092', max_block_ms=5000)
    producer.send('user_created', json.dumps(data).encode('utf-8'))

    #return data
   
   


# with DAG('user_develop',
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False) as dag:
#     streaming_task = PythonOperator(
#         task_id = "streaming_data_from_api",
#         python_callable=stream_data
#     )

stream_data()
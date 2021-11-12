from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson import json_util
import boto3

# Constants

path = '/usr/local/airflow/data'

#inserir keys aws
aws_access_key_id=''
aws_secret_access_key=''

default_args = {
    'owner': 'Rafael',
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 3, 23, 10),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'Desafio_Modulo_4_S3',
    description='ETL Desafio Final',
    default_args=default_args,
    schedule_interval='@once'
)


# ---------------------------------------- MONGODB ----------------------------------------

# get data from mongo filtering by man
def get_mongo_man_data():
    passDb = '' #inserir senha do banco de dados
    url = f"mongodb+srv://estudante_igti:{passDb}@unicluster.ixhvw.mongodb.net/admin?authSource=admin"

    client = MongoClient(url)

    database = client['ibge']
    collection = database["pnadc20203"]

    cursor = collection.find({"sexo": "Homem"})

    with open(f'{path}/tgt_mongo_homem.json', 'w') as file:
        for document in cursor:
                file.write(f'{json_util.dumps(document)}\n')


# get data from mongo filtering by women
def get_mongo_women_data():
    url = "mongodb+srv://estudante_igti:{passDb}@unicluster.ixhvw.mongodb.net/admin?authSource=admin"

    client = MongoClient(url)

    database = client['ibge']
    collection = database["pnadc20203"]

    cursor = collection.find({"sexo": "Mulher"})

    with open(f'{path}/tgt_mongo_mulher.json', 'w') as file:
        
        for document in cursor:
            file.write(f'{json_util.dumps(document)}\n')
                



# Upload final mongo target file on S3
def upload_mongo_file():
    session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    )   

    s3 = session.client('s3')
    s3.upload_file(f'{path}/tgt_mongo.json','igti-bootcamp-ed-2021-487532217566','tgt_mongo.json')



# Mongo tasks
task_get_mongo_man_data = PythonOperator(
    task_id='get_mongo_man_data',
    python_callable=get_mongo_man_data,
    dag=dag
)

task_get_mongo_women_data = PythonOperator(
    task_id='get_mongo_women_data',
    python_callable=get_mongo_women_data,
    dag=dag
)

task_join_mongo_data = BashOperator(
    task_id='join_mongo_data',
    bash_command=f'cat {path}/tgt_mongo_*.json > {path}/tgt_mongo.json',
    dag=dag
)

task_delete_mongo_tgt_files = BashOperator(
    task_id='delete_mongo_tgt_files',
    bash_command=f'rm {path}/tgt_mongo_*.json',
    dag=dag
)

task_upload_mongo_file = PythonOperator(
    task_id='upload_mongo_file',
    python_callable=upload_mongo_file,
    dag=dag
)


# ---------------------------------------- IBGE API ----------------------------------------

# get data from IBGE API 
get_regioes_command = f'curl https://servicodados.ibge.gov.br/api/v1/localidades/regioes -o {path}/regioes.txt'
get_mesorregioes_command = f'curl https://servicodados.ibge.gov.br/api/v1/localidades/mesorregioes -o {path}/mesorregioes.txt'
get_microrregioes_command = f'curl https://servicodados.ibge.gov.br/api/v1/localidades/microrregioes -o {path}/microrregioes.txt' 

# Upload final regioes file on S3
def upload_regioes_file():
    session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
    )   

    s3 = session.client('s3')
    s3.upload_file(f'{path}/regioes.txt','igti-bootcamp-ed-2021-487532217566','regioes.txt')

# Upload final mesorregioes file on S3
def upload_mesorregioes_file():
    session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    )   

    s3 = session.client('s3')
    s3.upload_file(f'{path}/mesorregioes.txt','igti-bootcamp-ed-2021-487532217566','mesorregioes.txt')


# Upload final microrregioes file on S3
def upload_microrregioes_file():
    session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    )   

    s3 = session.client('s3')
    s3.upload_file(f'{path}/microrregioes.txt','igti-bootcamp-ed-2021-487532217566','microrregioes.txt')

# API IBGE tasks
task_get_regioes = BashOperator(
    task_id='get_regioes',
    bash_command=get_regioes_command,
    dag=dag
)

task_get_mesorregioes = BashOperator(
    task_id='get_mesorregioes',
    bash_command=get_mesorregioes_command,
    dag=dag
)

task_get_microrregioes = BashOperator(
    task_id='get_microrregioes',
    bash_command=get_microrregioes_command,
    dag=dag
)

task_upload_regioes_file = PythonOperator(
    task_id='upload_regioes_file',
    python_callable=upload_regioes_file,
    dag=dag
)

task_upload_mesorregioes_file = PythonOperator(
    task_id='upload_mesorregioes_file',
    python_callable=upload_mesorregioes_file,
    dag=dag
)

task_upload_microrregioes_file = PythonOperator(
    task_id='upload_microrregioes_file',
    python_callable=upload_microrregioes_file,
    dag=dag
)

[task_get_mongo_man_data, task_get_mongo_women_data] >> task_join_mongo_data >> [task_delete_mongo_tgt_files, task_upload_mongo_file]
[task_get_regioes, task_get_mesorregioes, task_get_microrregioes]
task_get_regioes >> task_upload_regioes_file
task_get_mesorregioes >> task_upload_mesorregioes_file
task_get_microrregioes >> task_upload_microrregioes_file

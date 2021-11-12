import pandas as pd
import json
import numpy as np
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import boto3
import sqlalchemy


path = '/home/rafael/Documentos/desafio final/docker-airflow/data'
#enter aws info
aws_access_key_id=''
aws_secret_access_key=''
engine = sqlalchemy.create_engine(
    ""
    )

default_args = {
    'owner': 'Rafael',
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 7, 23, 10),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'Desafio_Modulo_4_DW',
    description='ETL Desafio Final',
    default_args=default_args,
    schedule_interval='@once'
)

def treat_mongo_data():
    persons = []
    with open(f'{path}/tgt_mongo.json', 'r') as file:
        for line in file:
            persons.append(json.loads(line))

    df = pd.DataFrame(persons)
    df['_id'] = df.apply(lambda e: e['_id']['$oid'], axis=1)
    df = df.replace({np.nan: None})
    df.rename(columns={'_id': 'id_dim_pessoa'}, inplace=True)
    df = df.loc[(df['idade'] >= 20) & (df['idade'] <= 40)]
    df = df.loc[df['sexo'] == 'Mulher']
    df.to_csv(f'{path}/mongo_tratado.csv', index=False)

def load_dim_lugar():
    df_dim_lugar = pd.DataFrame()
    df_mesorregiao = pd.read_json(f'{path}/mesorregioes.txt')
    df_mesorregiao['regiao'] = df_mesorregiao.apply(lambda e: e['UF']['regiao']['nome'], axis=1)
    df_mesorregiao['UF'] = df_mesorregiao.apply(lambda e: e['UF']['nome'], axis=1)
    mesorregiao_count = df_mesorregiao.groupby(['UF']).count().reset_index()
    df_dim_lugar = df_mesorregiao.merge(mesorregiao_count,on='UF')
    df_dim_lugar = df_dim_lugar.drop_duplicates(subset=['UF']).reset_index(drop=True).rename(columns={'regiao_x':'regiao', 'id_y':'qtd_mesorregioes', 'UF':'uf'})
    df_dim_lugar = df_dim_lugar[['uf','regiao','qtd_mesorregioes']]
    df_dim_lugar.insert(0, 'id_dim_lugar', df_dim_lugar.index)
    df_dim_lugar.to_sql('dim_lugar', con=engine, index=False, if_exists='replace')

def load_dim_tempo():
    df_dim_tempo = pd.DataFrame(columns=['ano', 'trimestre'])
    for i in range(2000,2101):
        for j in range(1,4):
            df_dim_tempo = df_dim_tempo.append({'ano':i, 'trimestre': j}, ignore_index=True)
    df_dim_tempo.insert(0, 'id_dim_tempo', df_dim_tempo.index)
    df_dim_tempo.to_sql('dim_tempo', con=engine, index=False, if_exists='replace')

def load_dim_pessoa():
    df = pd.read_csv(f'{path}/mongo_tratado.csv')
    df_dim_pessoa = df.drop(columns=['ano','trimestre','uf'])
    df_dim_pessoa.to_sql('dim_pessoa', con=engine, index=False, if_exists='replace')

def load_fato_pessoa():
    df = pd.read_csv(f'{path}/mongo_tratado.csv')
    df_fato_pessoa = df.drop(columns=['sexo', 'idade', 'cor', 'graduacao', 'trab', 'ocup', 'renda', 'horastrab', 'anosesco'])
    df_dim_lugar = pd.read_sql('dim_lugar',con=engine, columns=['id_dim_lugar','uf'])
    df_fato_pessoa = df_fato_pessoa.merge(df_dim_lugar, on='uf')
    df_fato_pessoa = df_fato_pessoa.drop(columns=['uf'])
    df_dim_tempo = pd.read_sql('dim_tempo',con=engine, columns=['id_dim_tempo','ano','trimestre'])
    df_fato_pessoa = df_fato_pessoa.merge(df_dim_tempo, on=['ano', 'trimestre'])
    df_fato_pessoa = df_fato_pessoa.drop(columns=['ano','trimestre'])
    df_fato_pessoa.to_sql('fato_pessoa', con=engine, index=False, if_exists='replace')

task_treat_mongo_data = PythonOperator(
    task_id='treat_mongo_data',
    python_callable=treat_mongo_data,
    dag=dag
)

task_load_dim_tempo = PythonOperator(
    task_id='load_dim_tempo',
    python_callable=load_dim_tempo,
    dag=dag
)

task_load_dim_pessoa = PythonOperator(
    task_id='load_dim_pessoa',
    python_callable=load_dim_pessoa,
    dag=dag
)

task_load_dim_lugar = PythonOperator(
    task_id='load_dim_lugar',
    python_callable=load_dim_lugar,
    dag=dag
)

task_load_fato_pessoa = PythonOperator(
    task_id='load_fato_pessoa',
    python_callable=load_fato_pessoa,
    dag=dag
)

task_delete_mongo_file = BashOperator(
    task_id='delete_mongo_file',
    bash_command=f'rm "{path}/mongo_tratado.csv"',
    dag=dag
)

[task_treat_mongo_data,task_load_dim_lugar,task_load_dim_tempo]
task_treat_mongo_data >> task_load_dim_pessoa >> task_delete_mongo_file
[task_load_dim_pessoa, task_load_dim_lugar, task_load_dim_tempo] >> task_load_fato_pessoa

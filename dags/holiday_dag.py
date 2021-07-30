from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import requests
import json
from airflow.operators.python_operator import PythonOperator
from google.oauth2 import service_account
from pandas_gbq import to_gbq


token = '' #token da API Calendarific, gere o seu em: https://calendarific.com/
credentials = service_account.Credentials.from_service_account_file('') #path para o arquivo .json, que contém a chave da conta de serviço do google cloud, mais informações em https://cloud.google.com/iam/docs/creating-managing-service-accounts

BQ_PROJECT = '' #id do projeto
BQ_DATASET = '' #id do dataset bigquery
BQ_TABLE = '' #id da table bigquery

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 29),
    'email': ['email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag2 = DAG(
    'holiday_dag',
    default_args=default_args,
    description='Atualização diária de feriados americanos',
    schedule_interval=timedelta(days=1)
)


def etl():    
    df = pd.read_csv('dags/countryContinent.csv', encoding = "ISO-8859-1") #acesso ao dataset de países e continentes em: https://www.kaggle.com/statchaitya/country-to-continent

    df_continentes = df.filter(['country', 'continent', 'code_2'], axis=1)
    df_america = df_continentes[df_continentes['continent'] == 'Americas']
    codes = df_america['code_2'].tolist()

    data_hoje = datetime.now()
    ano, mes, dia = data_hoje.year, data_hoje.month, data_hoje.day

    nomes_feriados = []
    descricao_feriados = []
    paises = []
    data_feriados = []

    #todo: comparação[] entre codes e lista de países da calendarific, pra diminuir o número de requests em apenas países suportados pela API

    for code in codes:
        r = requests.get("https://calendarific.com/api/v2/holidays?api_key={token}&country={code}&year={ano}&month={mes}&day={dia}".format(code=code, ano=ano,mes=mes, dia=dia, token=token))
        feriados = r.json()

        if not 'response' in feriados or len(feriados['response']) == 0:
            pass
        else:
            for feriado in feriados["response"]["holidays"]:
                nomes_feriados.append(feriado["name"])
                descricao_feriados.append(feriado["description"])
                paises.append(feriado["country"]["name"])
                data_feriados.append(feriado["date"]["iso"])


    dicionario_feriados = {
        "nomes_feriados": nomes_feriados,
        "descricao_feriados": descricao_feriados,
        "paises": paises,
        "datas_feriados": data_feriados
    }

    df_feriados = pd.DataFrame(dicionario_feriados, columns=["nomes_feriados", "descricao_feriados", "paises", "datas_feriados"])

    df_feriados["nomes_feriados"] = df_feriados["nomes_feriados"].astype(str)
    df_feriados["descricao_feriados"] = df_feriados["descricao_feriados"].astype(str)
    df_feriados["paises"] = df_feriados["paises"].astype(str)
    df_feriados["datas_feriados"] = df_feriados["datas_feriados"].astype(str)

    #validação
        #possibilidade de retorno com dataframe vazio
        #checar se existem valores nulos    
        
    if df_feriados.empty:
        print("Hoje não é feriado em nenhum país da America!")
        return False
    
    elif df_feriados[["nomes_feriados", "paises", "datas_feriados"]].isnull().values.any():
        raise Exception("Valor nulo encontrado")
    
    else:
        print("Valores validados! Pronto para armazenaemnto")

        #carregamento para a bigquery table
        df_feriados.to_gbq(BQ_TABLE, project_id= BQ_PROJECT, credentials=credentials, if_exists='append')

    pass



etl = PythonOperator(
    task_id="etl",
    python_callable=etl,
    dag=dag2
)


etl
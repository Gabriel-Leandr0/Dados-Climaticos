from airflow import DAG
import pendulum
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.macros import ds_add
from os.path import join
import pandas as pd
from datetime import datetime, timedelta

with DAG(
        "dados_climaticos",
        start_date=pendulum.datetime(2023, 4, 3, tz="UTC"),
        schedule_interval='0 0 * * 1',  # executar toda segunda feira
) as dag:
    cria_pasta = BashOperator(
        task_id='cria_pasta',
        bash_command='mkdir -p "/home/gabriel/Documents/airflow/semana={{data_interval_end.strftime("%Y-%m-%d")}}"'
    )


    def extrai_dados(data_interval_end):
        city = 'Santos-SP'
        key = 'R567426UHXH84M2TTZQHQBY44'

        # join foi usado apenas para quebrar a linha e manter organizado
        URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
                   f'{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv')

        dados = pd.read_csv(URL)

        file_path = f'/home/gabriel/Documents/airflow/semana={data_interval_end}/'

        # pipeline (filtrando dados finais)
        dados.to_csv(file_path + 'dados_brutos.csv')
        dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv')
        dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')


    dados_climaticos = PythonOperator(
        task_id='dados_climaticos',
        python_callable=extrai_dados,
        op_kwargs={'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    cria_pasta >> dados_climaticos
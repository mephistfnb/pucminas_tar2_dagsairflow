import pandas as pd

from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

TABUN = "/tmp/tabela_unica.csv"

default_args = {
    'owner': "FlavioBilancieri",
    "depends_on_past": False,
    'start_date': datetime (2022, 10, 13)
}

@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['Titanic'], dag_id = "dag_2_fnb")
def dag2_fnb():

    @task
    def prd_medias_indicadores():
        NOME_DO_ARQUIVO = "/tmp/resultados.csv"
        df = pd.read_csv(TABUN, sep=';')
        res = df.groupby(['PassengerId', 'Fare','SibPa']).agg({
            "PassengerId": "mean",
            "Fare": "mean",
            "SibPa": "mean"
        }).sum().reset_index()
        print(res)
        res.to_csv(NOME_DO_ARQUIVO, index=False, sep=";")
        return NOME_DO_ARQUIVO
    

    fim = DummyOperator(task_id="fim")
    
    res_tot = prd_medias_indicadores()

    res_tot >> fim

execucao = dag2_fnb()
        
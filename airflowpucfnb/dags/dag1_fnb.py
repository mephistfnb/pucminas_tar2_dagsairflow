import pandas as pd

from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"

default_args = {
    'owner': "FlavioBilancieri",
    "depends_on_past": False,
    'start_date': datetime (2022, 10, 13)
}

@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['Titanic'], dag_id = "dag_1_fnb")
def dag1_fnb():

    @task
    def ingestao():
        NOME_DO_ARQUIVO = "/tmp/titanic.csv"
        df = pd.read_csv(URL, sep=';')
        df.to_csv(NOME_DO_ARQUIVO, index=False, sep=";")
        return NOME_DO_ARQUIVO

    @task
    def ind_passageiros(nome_do_arquivo):
        NOME_TABELA_1 = "/tmp/passageiros_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res_1 = df.groupby(['Sex', 'Pclass']).agg({
            "PassengerId": "count"
        }).reset_index()
        print(res_1)
        res_1.to_csv(NOME_TABELA_1, index=False, sep=";")
        return NOME_TABELA_1

    @task
    def ind_tarifa(nome_do_arquivo):
        NOME_TABELA_2 = "/tmp/tarifas_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res_2 = df.groupby(['Sex', 'Pclass']).agg({
            "Fare": "mean"
        }).reset_index()
        print(res_2)
        res_2.to_csv(NOME_TABELA_2, index=False, sep=";")
        return NOME_TABELA_2

    @task
    def ind_sibpa(nome_do_arquivo):
        NOME_TABELA_3 = "/tmp/sibpa_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        sum_column = df["SibSp"] + df["Parch"]
        df["SibPa"] = sum_column
        res_3 = df.groupby(['Sex', 'Pclass'])['SibPa'].sum().reset_index()
        print(res_3)
        res_3.to_csv(NOME_TABELA_3, index=False, sep=";")
        return NOME_TABELA_3

    @task
    def ind_tabun():
        NOME_TABELA_4 = "/tmp/tabela_unica.csv"
        df1 = pd.read_csv("/tmp/tarifas_por_sexo_classe.csv", sep=";")
        df2 = pd.read_csv("/tmp/passageiros_por_sexo_classe.csv", sep=";") 
        df3 = pd.read_csv("/tmp/sibpa_por_sexo_classe.csv", sep=";") 
        res = pd.concat([df1,df2,df3], axis=1)
        print(res)
        res.to_csv(NOME_TABELA_4, index=False, sep=";")
        return NOME_TABELA_4
    
    triggerdag = TriggerDagRunOperator(
        task_id="trigga_dag_2_fnb",
        trigger_dag_id="dag_2_fnb"
    )

    fim = DummyOperator(task_id="fim")

    ing = ingestao()
    indicador_pass = ind_passageiros(ing)
    indicador_fare = ind_tarifa(ing)
    indicador_sibpa = ind_sibpa(ing)
    indicador_tabun = ind_tabun()

    indicador_pass >> indicador_fare >> indicador_sibpa >> indicador_tabun >> fim

    fim >> triggerdag

execucao = dag1_fnb()
        
import datetime
import pendulum
import os

import requests as r
import unidecode
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    dag_id="etl_ibge_base_data",
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 8, 17, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def etlIbgeMunicipios():
    @task
    def extractMunicipios():
        postgres_hook = PostgresHook(postgres_conn_id="weather_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(f"""select id_ibge, regiao_id from tbl_estados;""")
        
        estados = cur.fetchall()
        
        all_data = []
        for estado in estados:
            res = r.get(f'https://servicodados.ibge.gov.br/api/v1/localidades/estados/{estado[0]}/municipios')
            data = res.json()
            for line in data:
                line['estado_id'] = estado[0]
            all_data = all_data + data
            
        return all_data
        
    @task
    def transformData(data):
        new_data = []
        for row in data:
            new_data.append({
                'id_ibge': row['id'],
                'nome': unidecode.unidecode(row['nome']).replace("'", ""),
                'estado_id': row['estado_id'],
                'microrregiao_id': row['microrregiao']['id'],
                'mesorregiao_id': row['microrregiao']['mesorregiao']['id'],
                'uf_id': row['microrregiao']['mesorregiao']['UF']['id']
            })
        
        print(new_data)
        return new_data
    
    @task
    def extractMesorregioes():
        res = r.get('https://servicodados.ibge.gov.br/api/v1/localidades/mesorregioes')
        data = res.json()
                
        data_formatted = []
        for row in data:
            data_formatted.append({'nome': unidecode.unidecode(row['nome']).replace("'", ""), 'id': row['id']})
            
        del res
        return data_formatted

    @task
    def loadMesorregioes(data):
        query = "INSERT INTO tbl_mesorregioes (id_ibge, nome) VALUES "
        for row in data:
            query = query + f"({row['id']}, '{row['nome']}'),"
            
        postgres_hook = PostgresHook(postgres_conn_id="weather_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute("truncate tbl_mesorregioes;")
        cur.execute(query[:len(query)-1])
        conn.commit()

    @task
    def extractMicrorregioes():
        res = r.get('https://servicodados.ibge.gov.br/api/v1/localidades/microrregioes')
        data = res.json()
                
        data_formatted = []
        for row in data:
            data_formatted.append({'nome': unidecode.unidecode(row['nome']).replace("'", ""), 'id': row['id']})
            
        del res
        return data_formatted

    @task
    def loadMicrorregioes(data):
        query = "INSERT INTO tbl_microrregioes (id_ibge, nome) VALUES "
        for row in data:
            query = query + f"({row['id']}, '{row['nome']}'),"
            
        postgres_hook = PostgresHook(postgres_conn_id="weather_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute("truncate tbl_microrregioes;")
        cur.execute(query[:len(query)-1])
        conn.commit()
        
    @task
    def loadData(data):
        query = "INSERT INTO tbl_municipios (id_ibge, nome, estado_id, mesorregiao_id, microrregiao_id) VALUES "
        for row in data:
            query = query + f"({row['id_ibge']}, '{row['nome']}', {row['estado_id']}, {row['mesorregiao_id']}, {row['microrregiao_id']}), "
            
        postgres_hook = PostgresHook(postgres_conn_id="weather_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute("truncate tbl_municipios;")
        cur.execute(query[:len(query)-2])
        conn.commit()
        
    loadData(transformData(extractMunicipios()))
    
    loadMesorregioes(extractMesorregioes())
    
    loadMicrorregioes(extractMicrorregioes())
    
dag = etlIbgeMunicipios()
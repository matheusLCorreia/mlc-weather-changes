import datetime
import pendulum
import os
import requests
import re
import unidecode
import Helpers as h

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    dag_id="etl_gov_weather",
    schedule=None,#"0 * * * *",
    start_date=pendulum.datetime(2024, 8, 17, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def EtlGovWeather():
    
    @task
    def extractDataFiles():
        year = '2022'
        base_path = f'/opt/data_files/gov_weather_data_hist/{year}'
        files = os.listdir(base_path)
        
        postgres_hook = PostgresHook(postgres_conn_id="weather_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(f"""DELETE from tbl_clima_hist where extract(year from "data") = cast({year} as integer);""")
        conn.commit()
        
        data = {'data': list(), 'year': year}
        for file in files[:]:
            string_match = re.search('INMET_(.*)_(.*)_(.*)_(.*)_(.*)_.*_(.*).CSV', file)
            
            city = string_match.group(4)
            print(f"{city}, {string_match.group(5)}, {string_match.group(6)}")
            print(f"=> {file}")
            
            data['data'] = transformDataFromCSV(f"{base_path}/{file}")
            
            print("DATA SIZE: ", len(data['data']))
            
            loadDataDB(data)
            # break
        return 1

    def transformDataFromCSV(file):
        try:
            with open(file, 'r', encoding='ISO-8859-1') as fl:
                raw = fl.read().split('\n')
        except Exception as error:
            print("Failed to open history csv.")
            print(error)
            exit(1)
            
        try:
            info = raw[:8]
            lat = float(info[4].replace('LATITUDE:;', '').replace(',', '.'))
            lon = float(info[5].replace('LONGITUDE:;', '').replace(',', '.'))
            estacao = info[2].replace('ESTACAO:;', '')
            
            all_data = raw[9:]
            data_formatted = []
            print("aqui 1")
            city = h.extractCityByCoordGoogle(lat, lon)
            city = unidecode.unidecode(city[0]).replace("'", "")
        
        except Exception as error:
            print("Failed to format data to create dict. ")
            print(error)
            exit(2)
            
        try:    
            postgres_hook = PostgresHook(postgres_conn_id="weather_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(f"select id_ibge from tbl_municipios where nome ilike ('%{city}%');")
            data = cur.fetchall()
            
            city_id = 0
            if len(data) > 0 and len(data[0]) > 0:
                city_id = data[0][0]
        except Exception as error:
            print("Getting city id.")
            print(error)
            exit(3)
        
        for row in all_data:
            row = row.split(';')
            if len(row) < 2:
                continue
            try:
                data_formatted.append({
                    'municipio_id': city_id, 'estacao': estacao, 'data': h.formatDate(row[0]),
                    'hora': h.getHour(row[1]), 'latitude': lat, 'longitude': lon,
                    'precipitacao_total': h.formatTo(row[2].replace(',', '.')),
                    'pressao_atm_estacao': h.formatTo(row[3].replace(',', '.')),
                    'pressao_atm_max_lasthour': h.formatTo(row[4].replace(',', '.')),
                    'pressao_atm_min_lasthour': h.formatTo(row[5].replace(',', '.')),
                    'radicao_global': h.formatTo(row[6].replace(',', '.')),
                    'temp_ar': h.formatTo(row[7].replace(',', '.')),
                    'temp_ponto_orvalho': h.formatTo(row[8].replace(',', '.')),
                    'temp_max_lasthour': h.formatTo(row[9].replace(',', '.')),
                    'temp_min_lasthour': h.formatTo(row[10].replace(',', '.')),
                    'temp_orvalho_max_lasthour': h.formatTo(row[11].replace(',', '.')),
                    'temp_orvalho_min_lasthour': h.formatTo(row[12].replace(',', '.')),
                    'umid_rel_max_lasthour': h.formatTo(row[13].replace(',', '.')),
                    'umid_rel_min_lasthour': h.formatTo(row[14].replace(',', '.')),
                    'umid_rel_ar': h.formatTo(row[15].replace(',', '.')),
                    'vento_direcao': h.formatTo(row[16].replace(',', '.')),
                    'vento_rajada_max': h.formatTo(row[17].replace(',', '.')),
                    'vento_velocidade': h.formatTo(row[18].replace(',', '.'))
                })
            except Exception as error:
                print(error, row)
                exit(3)
        return data_formatted
    
    # @task
    def loadDataDB(data):
        postgres_hook = PostgresHook(postgres_conn_id="weather_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        print("Inserting data on DB...")
        try:
            query = """INSERT INTO public.tbl_clima_hist
            (municipio_id, estacao, "data", hora, latitude, longitude, precipitacao_total, pressao_atm_estacao, pressao_atm_max_lasthour, pressao_atm_min_lasthour, radicao_global, temp_ar, temp_ponto_orvalho, temp_max_lasthour, temp_min_lasthour, temp_orvalho_max_lasthour, temp_orvalho_min_lasthour, umid_rel_max_lasthour, umid_rel_min_lasthour, umid_rel_ar, vento_direcao, vento_rajada_max, vento_velocidade, created_at) VALUES """
            
            for row in data['data']:
                query = query + f"({row['municipio_id']}, '{row['estacao']}', '{row['data']}', '{row['hora']}', {row['latitude']}, {row['longitude']}, {row['precipitacao_total']}, {row['pressao_atm_estacao']}, {row['pressao_atm_max_lasthour']}, {row['pressao_atm_min_lasthour']}, {row['radicao_global']}, {row['temp_ar']}, {row['temp_ponto_orvalho']}, {row['temp_max_lasthour']}, {row['temp_min_lasthour']}, {row['temp_orvalho_max_lasthour']}, {row['temp_orvalho_min_lasthour']}, {row['umid_rel_max_lasthour']}, {row['umid_rel_min_lasthour']}, {row['umid_rel_ar']}, {row['vento_direcao']}, {row['vento_rajada_max']}, {row['vento_velocidade']}, current_timestamp),"
                
            # cur.execute(f"""DELETE from tbl_clima_hist where extract(year from "data") = cast({data['year']} as integer);""")
            print(query)
            cur.execute(query[:len(query)-1])
            conn.commit()
        except Exception as error:
            print(error)
            exit(3)
    
    extractDataFiles()
    # loadDataDB(data)


dag = EtlGovWeather()
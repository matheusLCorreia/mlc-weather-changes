import datetime
import pendulum
import os

import requests as r
import unidecode
import Helpers as h

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    dag_id="etl_weather_api",
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2024, 8, 17, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def etlWeatherAPI():
    @task
    def getCities():
        postgres_hook = PostgresHook(postgres_conn_id="weather_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(f"""select m.nome, e.sigla, m.id_ibge
        from tbl_municipios m
        inner join tbl_estados e on e.id_ibge = m.estado_id;
        --where e.sigla = 'SP'""")
        
        cities = cur.fetchall()
        return cities
       
    @task
    def extractWeatherData(cities):
        env = h.loadEnvironment()
        try:
            for city in cities:
                res = r.get(url=f"{env['WeatherAPI']['weather_api_base_path']}/current.json?key={env['WeatherAPI']['weather_api_token']}&q={city}&aqi=yes")
                print(res.status_code, res)
                
                if res.status_code == 200:
                    data = res.json()
                    data_location, data_weather, data_air = transformData(data['location'], data['current'])
                    print(data_location)
                    print("------")
                    print(data_weather)   
                    data_transformed = adjustDataDict(city[2], data_location, data_weather)
                    loadDataDB(data_transformed)
                else:
                    print("Fail on loading data... ", city)
                    exit(2)
                
        except (r.ConnectionError, r.HTTPError, r.RequestException) as conn_htt_err:
            print("Fail on Weather API connection/request. ", conn_htt_err)
            exit(1)
        except (r.Timeout) as timeout_err:
            print("Timeout on Weather API request. ", timeout_err)
            exit(2)
        
    def transformData(location, current_weather):
        location_source_fields = ['name', 'region', 'lat', 'lon', 'localtime_epoch']
        data_location = {}
        
        weather_source_fields = ['last_updated_epoch', 'temp_c', 'wind_kph', 'wind_degree', 'wind_dir', 'pressure_mb', 'pressure_in', 'precip_mm', 'precip_in', 'humidity', 'cloud', 'feelslike_c', 'windchill_c', 'heatindex_c', 'dewpoint_c', 'vis_km', 'uv', 'gust_kph']
        data_weather = {}
        
        air_source_fields = ['co', 'no2', 'o3', 'so2', 'pm2_5', 'pm10', 'us-epa-index', 'gb-defra-index']
        data_air = {}
        
        air_quality = current_weather['air_quality']
        
        for field in location_source_fields:
            data_location[field] = location[field]
            
        for field in weather_source_fields:
            data_weather[field] = current_weather[field]
        
        for field in air_source_fields:
            data_air[field] = air_quality[field]
                           
        return data_location, data_weather, data_air
    
    def loadDataDB(data):
        postgres_hook = PostgresHook(postgres_conn_id="weather_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        
        print("Inserting data on DB...")
        print(data)
        try:
            query = """INSERT INTO public.tbl_clima_lastest
            (municipio_id, estacao, "data", hora, latitude, longitude, precipitacao_total, pressao_atm_estacao, pressao_atm_max_lasthour, pressao_atm_min_lasthour, radicao_global, temp_ar, temp_ponto_orvalho, temp_max_lasthour, temp_min_lasthour, temp_orvalho_max_lasthour, temp_orvalho_min_lasthour, umid_rel_max_lasthour, umid_rel_min_lasthour, umid_rel_ar, vento_direcao, vento_rajada_max, vento_velocidade, created_at) VALUES """
            
            for row in data:
                query = query + f"({row['municipio_id']}, '{row['estacao']}', '{row['data']}', '{row['hora']}', {row['latitude']}, {row['longitude']}, {row['precipitacao_total']}, {row['pressao_atm_estacao']}, {row['pressao_atm_max_lasthour']}, {row['pressao_atm_min_lasthour']}, {row['radicao_global']}, {row['temp_ar']}, {row['temp_ponto_orvalho']}, {row['temp_max_lasthour']}, {row['temp_min_lasthour']}, {row['temp_orvalho_max_lasthour']}, {row['temp_orvalho_min_lasthour']}, {row['umid_rel_max_lasthour']}, {row['umid_rel_min_lasthour']}, {row['umid_rel_ar']}, {row['vento_direcao']}, {row['vento_rajada_max']}, {row['vento_velocidade']}, current_timestamp),"
                
            # cur.execute(f"""DELETE from tbl_clima_hist where extract(year from "data") = cast({data['year']} as integer);""")
            print(query)
            cur.execute(query[:len(query)-1])
            conn.commit()
        except Exception as error:
            print(error)
            exit(3)
            
    def adjustDataDict(city_id, location, weather):
        data_formatted = []
        estacao = 'WEATHER_API'
                
        row = weather
        print("=========", row)
        date, time = h.getDateHourByEpoch(int(row['last_updated_epoch']))
        data_formatted.append({
            'municipio_id': city_id, 'estacao': estacao, 'data': date,
            'hora': time, 'latitude': location['lat'], 'longitude': location['lon'],
            'precipitacao_total': row['precip_mm'],
            'pressao_atm_estacao': row['pressure_mb'],
            'pressao_atm_max_lasthour': 'null',
            'pressao_atm_min_lasthour': 'null',

            'radicao_global': 'null',
            'temp_ar': row['temp_c'],

            'temp_ponto_orvalho': row['dewpoint_c'],
            'temp_max_lasthour': row['temp_c'],
            'temp_min_lasthour': 'null',

            'temp_orvalho_max_lasthour': 'null',
            'temp_orvalho_min_lasthour': 'null',

            'umid_rel_max_lasthour': row['humidity'],
            'umid_rel_min_lasthour': 'null',
            'umid_rel_ar': row['humidity'],

            'vento_direcao': row['wind_degree'],
            'vento_rajada_max': float(row['gust_kph'])*3.6,
            'vento_velocidade': float(row['wind_kph'])*3.6
        })
            
        return data_formatted
            
    cities = getCities()
    extractWeatherData(cities)
    
dag = etlWeatherAPI()
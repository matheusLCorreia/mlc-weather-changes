import Connections as conn
import WeatherAPI as wa
import DataTransformations as dt
import datetime
import os
import re

class Controller:
    
    def __init__(self):
        print("Controller class...")
    
    def loadWeatherApiRealtime(self, city):
        print("loadWeatherApi method..")
        wa_obj = wa.WeatherAPI()
        data = wa_obj.loadRealtimeData(city)
        
        dt_obj = dt.DataTransformations()
        return dt_obj.currentWeatherData(data['location'], data['current'])
        
    def insertCurrentWeatherData(self, data, city):
        dw = conn.DW()
        con, cur = dw.connectDatabase()
        
        cur.execute(f"""select id_ibge
        from tbl_municipios
        where nome ilike ('%{city}%');""")
        
        data_id_city = cur.fetchall()
        data_id_city = data_id_city[0][0]
        
        if type(data_id_city) != int:
            print("Values is not an INT value. ERROR: 100820242128.")
        
        query_weather = """INSERT INTO public.tbl_clima_lastest (municipio_id, estacao, "data", hora, latitude, longitude, precipitacao_total, pressao_atm_estacao, pressao_atm_max_lasthour, pressao_atm_min_lasthour, radicao_global, temp_ar, temp_ponto_orvalho, temp_max_lasthour, temp_min_lasthour, temp_orvalho_max_lasthour, temp_orvalho_min_lasthour, umid_rel_max_lasthour, umid_rel_min_lasthour, umid_rel_ar, vento_direcao, vento_rajada_max, vento_velocidade, created_at) VALUES """
        
        date = datetime.datetime.fromtimestamp(data['weather']['last_updated_epoch']).strftime('%Y-%m-%d')
        time = datetime.datetime.fromtimestamp(data['weather']['last_updated_epoch']).strftime('%H:%M')
        query_weather = query_weather + f"({data_id_city}, 'WEATHER_API', '{date}', '{time}', {data['location']['lon']}, {data['location']['lat']}, {data['weather']['precip_mm']}, {data['weather']['pressure_mb']}, {data['weather']['pressure_mb']}, {data['weather']['pressure_mb']}, 0, 0, 0, 0, 0, 0, 0, {data['weather']['humidity']}, {data['weather']['humidity']}, {data['weather']['humidity']}, {data['weather']['wind_degree']}, {data['weather']['gust_kph']*3.6}, {data['weather']['wind_kph']*3.6}, current_timestamp);"
        
        cur.execute(query_weather)
        con.commit()
        con.close()
        
    def extractFilesHistoryWeatherGov(self):
        transform = dt.DataTransformations()
        base_path = 'C:\\Users\\mathe\\Documents\\estudos\\engenharia_de_dados\\project_tcc_simple\\2022'
        files = os.listdir(base_path)
        postgres = conn.DW()
        con, cur = postgres.connectDatabase()
        
        for file in files[:10]:
            string_match = re.search('INMET_(.*)_(.*)_(.*)_(.*)_(.*)_.*_(.*).CSV', file)
            
            city = string_match.group(4)
            print(f"{city}, {string_match.group(5)}, {string_match.group(6)}")
            print(f"=> {file}")
                                    
            data = transform.extractInmetDataFromCSV(f"{base_path}\\{file}", cur)
            ## INSERIR DADO NO BANCO
            break
        
        cur.close()
        con.close()
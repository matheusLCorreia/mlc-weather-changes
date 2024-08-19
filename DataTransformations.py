import Helpers as h
import unidecode

class DataTransformations:
    def __init__(self):
        print("")
        
    def currentWeatherData(self, location, current_weather):
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
            
        # print(data_location)
        # print("==========")
        # print(data_weather)
        # print("==========")
        # print(data_air)
               
        return data_location, data_weather, data_air
    
    def formatDate(self, value: str):
        value = value.split('/')
        year = value[0]
        month = value[1]
        day = value[2]
        
        return f"{year}-{month}-{day}"
    
    def getHour(self, value: str):
        value = value[:4]
        return f"{value[:2]}:{value[2:]}:00"
    
    def extractInmetDataFromCSV(self, file, cur_pgs):
        try:
            with open(file, 'r') as fl:
                raw = fl.read().split('\n')
        except Exception as error:
            print("Failed to open history csv.")
            print(error)
            return 1
            
        try:
            info = raw[:8]
            lat = float(info[4].replace('LATITUDE:;', '').replace(',', '.'))
            lon = float(info[5].replace('LONGITUDE:;', '').replace(',', '.'))
            estacao = info[2].replace('ESTACAO:;', '')
                    
            all_data = raw[9:]
            data_formatted = []
            
            city = h.extractCityByCoordGoogle(lat, lon)
            city = unidecode.unidecode(city[0]).replace("'", "")
            cur_pgs.execute(f"select id_ibge from tbl_municipios where nome ilike ('%{city}%');")
            data = cur_pgs.fetchall()
        
            city_id = 0
            if len(data[0]) > 0:
                city_id = data[0][0]
        except Exception as error:
            print("Failed to format data to create dict.")
            print(error)
            return 2
        
        for row in all_data:
            row = row.split(';')
            if len(row) < 2:
                continue
            try:
                data_formatted.append({
                    'municipio_id': city_id,
                    'estacao': estacao,
                    'data': self.formatDate(row[0]),
                    'hora': self.getHour(row[1]),
                    'latitude': lat,
                    'longitude': lon,
                    'precipitacao_total': row[2].replace(',', '.'),
                    'pressao_atm_estacao': row[3].replace(',', '.'),
                    'pressao_atm_max_lasthour': row[4].replace(',', '.'),
                    'pressao_atm_min_lasthour': row[5].replace(',', '.'),
                    'radicao_global': row[6].replace(',', '.'),
                    'temp_ar': row[7].replace(',', '.'),
                    'temp_ponto_orvalho': row[8].replace(',', '.'),
                    'temp_max_lasthour': row[9].replace(',', '.'),
                    'temp_min_lasthour': row[10].replace(',', '.'),
                    'temp_orvalho_max_lasthour': row[11].replace(',', '.'),
                    'temp_orvalho_min_lasthour': row[12].replace(',', '.'),
                    'umid_rel_max_lasthour': row[13].replace(',', '.'),
                    'umid_rel_min_lasthour': row[14].replace(',', '.'),
                    'umid_rel_ar': row[15].replace(',', '.'),
                    'vento_direcao': row[16].replace(',', '.'),
                    'vento_rajada_max': row[17].replace(',', '.'),
                    'vento_velocidade': row[18].replace(',', '.')
                })
            except Exception as error:
                print(error, row)
        return data_formatted
    
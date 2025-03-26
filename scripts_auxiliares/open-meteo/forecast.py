import requests as req
import psycopg2
import datetime as dt

# years = [2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023]
years = [2024]
year = 0
def connectDatabase():
    try:
        with psycopg2.connect(f"""host=localhost dbname=weather_db port=5432 user=postgres password=18223005""") as conn:
            cur = conn.cursor()
            return conn, cur
    except (psycopg2.Error, psycopg2.DatabaseError) as pg:
        print(pg)
        exit(1)
    return None, None

def getWeatherHist(lat, lon):
	res = req.get(f'https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m,relative_humidity_2m,apparent_temperature,surface_pressure,precipitation,rain,shortwave_radiation,direct_radiation,diffuse_radiation,wind_speed_10m,wind_direction_10m,wind_gusts_10m,soil_temperature_0_to_7cm,soil_moisture_0_to_7cm,wind_gusts_10m&wind_speed_unit=ms&timeformat=unixtime&timezone=America%2FSao_Paulo')

	data = res.json()
	print(data)
	return data

def loadCities():
    conn, cur = connectDatabase()
    cur.execute("""select m.nome, m.id_ibge, m.longitude, m.latitude, m.nome
	from tbl_municipios m
	inner join tbl_estados e on e.id_ibge = m.estado_id
	where e.sigla = 'SP'
	and lower(m.nome) ~ '^(adamantina)$' --|barra do turvo|barretos|cajuru|campinas|hortolandia|morro agudo|olimpia|paulinia|piracicaba|ribeirao preto|sao paulo)$'
	order by 5 asc; """)
    
    data = cur.fetchall()
    cur.close()
    conn.close()
    
    return data

def formatWeatherData(weather_data):
    if 'error' in weather_data:
        print(f"ERROR: {weather_data}")
        return 1
    if 'hourly' not in weather_data:
        print(weather_data)
        return 1
    weather_data = weather_data['hourly']
    _keys = list(dict(weather_data).keys())
    
    all_data = []
    for i in range(len(weather_data[_keys[0]])):
        line = {}
        for k in _keys:
            line[k] = weather_data[k][i]
        
        all_data.append(line)
    
    return all_data

def insertData(data, city_id):
	query = """INSERT INTO public.tbl_open_weather_forecast (municipio_id, temperature_2m, relative_humidity_2m, apparent_temperature, surface_pressure, precipitation, rain, shortwave_radiation, direct_radiation, diffuse_radiation, wind_speed_10m, wind_direction_10m, wind_gusts_10m, soil_temperature_0_to_7cm, soil_moisture_0_to_7cm, date_time) VALUES """

	for row in data:
		if row['temperature_2m'] == None:
			print(row)
			continue
		# print(f"({city_id}, {row['temperature_2m']}, {row['relative_humidity_2m']}, {row['apparent_temperature']}, {row['surface_pressure']}, {row['precipitation']}, {row['rain']}, {row['shortwave_radiation']}, {row['direct_radiation']}, {row['diffuse_radiation']}, {row['wind_speed_10m']}, {row['wind_direction_10m']}, {row['wind_gusts_10m']}, {row['soil_temperature_0_to_7cm']}, {row['soil_moisture_0_to_7cm']}, {row['time']}),")
		query = query + f"({city_id}, {row['temperature_2m']}, {row['relative_humidity_2m']}, {row['apparent_temperature']}, {row['surface_pressure']}, {row['precipitation']}, {row['rain']}, {row['shortwave_radiation']}, {row['direct_radiation']}, {row['diffuse_radiation']}, {row['wind_speed_10m']}, {row['wind_direction_10m']}, {row['wind_gusts_10m']}, {row['soil_temperature_0_to_7cm']}, {row['soil_moisture_0_to_7cm']}, {row['time']}),".replace('None', 'null')

	print(query)
	if len(data) >= 1:
		return query[:len(query)-1]

def main():
	print(dt.datetime.now())
	cities = loadCities()

	conn, cur = connectDatabase()
	for city in cities[:]:
		print(city)
		# cur.execute(f"delete from tbl_open_weather_hist where municipio_id = {city[1]} and date_time between ")
		data = formatWeatherData(getWeatherHist(city[3], city[2]))
		if type(data) == int and data == 1:
			print("aqui")
			continue
		print(f"===>>> {len(data)}")
		cur.execute(f"delete from tbl_open_weather_forecast where municipio_id = {city[1]};")		
		cur.execute(insertData(data, city[1]))
		conn.commit()
		# break
	cur.close()
	conn.close()  
	print(dt.datetime.now())

main()
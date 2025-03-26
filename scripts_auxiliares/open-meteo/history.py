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
	res = req.get(f'https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date={year}-01-01&end_date={year}-10-31&hourly=temperature_2m,relative_humidity_2m,apparent_temperature,surface_pressure,precipitation,rain,shortwave_radiation,direct_radiation,diffuse_radiation,wind_speed_10m,wind_direction_10m,wind_gusts_10m,soil_temperature_0_to_7cm,soil_moisture_0_to_7cm,wind_gusts_10m&wind_speed_unit=ms&timeformat=unixtime&timezone=America%2FSao_Paulo')

	data = res.json()
	return data

def loadCities():
    conn, cur = connectDatabase()
    cur.execute(f"""select m1.nome, m1.id_ibge, m1.longitude, m1.latitude, m1.nome
	from tbl_municipios m1
	where estado_id = 35 and m1.nome not in (select m.nome
	from tbl_open_weather_hist h
	inner join tbl_municipios m on m.id_ibge = h.municipio_id
	where extract(year from to_timestamp(h.date_time)) = {year} and m.nome = 'Campinas'
	group by m.nome order by m.nome asc) and m1.nome = 'Campinas'
	order by m1.nome asc limit 5;""")
    
    data = cur.fetchall()
    cur.close()
    conn.close()
    
    return data

def formatWeatherData(weather_data):
    if 'error' in weather_data:
        print(f"ERROR: {weather_data}")
        exit()
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
	query = """INSERT INTO public.tbl_open_weather_hist (municipio_id, temperature_2m, relative_humidity_2m, apparent_temperature, surface_pressure, precipitation, rain, shortwave_radiation, direct_radiation, diffuse_radiation, wind_speed_10m, wind_direction_10m, wind_gusts_10m, soil_temperature_0_to_7cm, soil_moisture_0_to_7cm, date_time) VALUES """

	for row in data:
		if row['temperature_2m'] == None:
			print(row)
			continue
		query = query + f"({city_id}, {row['temperature_2m']}, {row['relative_humidity_2m']}, {row['apparent_temperature']}, {row['surface_pressure']}, {row['precipitation']}, {row['rain']}, {row['shortwave_radiation']}, {row['direct_radiation']}, {row['diffuse_radiation']}, {row['wind_speed_10m']}, {row['wind_direction_10m']}, {row['wind_gusts_10m']}, {row['soil_temperature_0_to_7cm']}, {row['soil_moisture_0_to_7cm']}, {row['time']}),"

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
		cur.execute(insertData(data, city[1]
			)
        )
		conn.commit()

	cur.close()
	conn.close()  
	print(dt.datetime.now())

for y in years:
	year = y
	print(f"======= {year} =======")
	main()
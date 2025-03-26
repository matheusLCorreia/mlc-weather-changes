import Controller
import datetime as dt
import psycopg2 as pg2    
import Helpers as h
import unidecode
from tqdm import tqdm
from multiprocessing import Pool
import threading
import time
def init():
    controller = Controller.Controller()
    cities = ['campinas', 'paulinia', 'valinhos']
    
    for city in cities:
        print(f"{dt.datetime.now()}; Extracting current weather data to {city}.")
        location, weather, air = controller.loadWeatherApiRealtime(city)
        controller.insertCurrentWeatherData({'location': location, 'weather': weather, 'air': air}, city)
    
# init()


def action(row):
    query = ""
    # print(row)
    # for row in all_data[:]:
    try:
        city_name, state = h.extractCityByCoordGoogle(row[1], row[0])
        city_name = unidecode.unidecode(city_name).replace("'", "")
    except Exception as error:
        print(error)
        time.sleep(3)
        city_name, state = h.extractCityByCoordGoogle(row[1], row[0])
        city_name = unidecode.unidecode(city_name).replace("'", "")
    
    city_id = 0           
    for city in cidades:
        if city_name.lower() == city[1].lower():
            city_id = city[0]
            break
    
    # print(city_name, state, city_id)
    if city_id != 0:
        conn, cur = connect()
        query = f"UPDATE tbl_incendios_hist set municipio_id = {city_id} where longitude = {row[0]} and latitude = {row[1]};"
        print(query)
        try:
            cur.execute(query)
            conn.commit()
        except Exception as error:
            print(error)
            time.sleep(3)
            cur.execute(query)
            conn.commit()
        
        cur.close()
        conn.close()
        
def update(offset):
    conn, cur = connect()
    cur.execute(f"SELECT longitude, latitude, date_time from tbl_incendios_hist where municipio_id isnull order by date_time asc limit 50 offset {offset};")
    all_data = cur.fetchall()
    cur.close()
    conn.close()
    
    try:
        threads = list()
        for i in range(len(all_data)):
            
            x = threading.Thread(target=action, args=(all_data[i],))
            threads.append(x)
            x.start()

        # for index, thread in enumerate(threads):
        #     # print("Main    : before joining thread %d.", index)
        #     thread.join()
        #     # print("Main    : thread %d done", index)
    except Exception as error:
        print(error)
   
def connect():
    with pg2.connect(f"""host=44.222.226.154 
            dbname=weather_db 
            port=5432
            user=postgres
            password=18223005""") as conn:
        
        cur = conn.cursor()
        
    return conn, cur

conn0, cur0 = connect()
cur0.execute(f"select id_ibge, nome from tbl_municipios order by id_ibge asc;")
cidades = cur0.fetchall()
cur0.close()
conn0.close()

i = 0
offset = 0
while i < 100000    :
    update(offset)
    offset = offset + 50
    i = i + 1
    print("===", i)

import requests as r
import psycopg2
import unidecode
import pandas as pd


def connectDatabase():
    conn = psycopg2.connect("host=localhost dbname=weather_db port=5432 user=postgres password=18223005")
    cur = conn.cursor()
    
    return conn, cur

def insertHistory(data):
    conn, cur = connectDatabase()
    query = "INSERT INTO tbl_incendios_hist (longitude, latitude, date_time, municipio_id, bioma) VALUES "
    for row in data:
        # mun_id = 'null'# loadCityLonLat(row['lat'], row['lon'])
        # print(row['estado'] == 'SÃO PAULO')
        if row['estado'] == 'SÃO PAULO':
            mun_id = 0
            for city in cities:
                # if unidecode.unidecode(city[2].lower()).replace("'", "") == 'irapua':
                # print(unidecode.unidecode(row['municipio']).lower().strip(),'====', city[2].lower())
                if unidecode.unidecode(row['municipio']).lower().strip() == city[2].lower():
                    mun_id = city[3]
                    break
            
            query += f"({row['lon']}, {row['lat']}, '{row['data_pas']}', {mun_id}, '{unidecode.unidecode(row['bioma']).lower().strip()}'),"
        
    
    cur.execute(query[:len(query)-1])
    conn.commit()
    cur.close()
    conn.close()

def loadCityLonLat(lat, lon):
    # res = req.get(f"https://api.geoapify.com/v1/geocode/search?text={city_name}&apiKey=82931b68d4564b398344638fbb113676")
    res = r.get(f"https://api.geoapify.com/v1/geocode/reverse?lat={lat}&lon={lon}&apiKey=82931b68d4564b398344638fbb113676")
    # print(res.status_code)
    
    data = res.json()
    city_name = ''
    if 'city' in data['features'][0]['properties'].keys():
        city_name = data['features'][0]['properties']['city']
        city_name = unidecode.unidecode(city_name).replace("'", "")
    
    if city_name != '':
        for city in cities:
            if city[2].lower() == city_name.lower():
                return city[3]
    return -1

def loadCities():
    global cities
    conn, cur = connectDatabase()
    cur.execute("""select e.nome, e.sigla, m.nome, m.id_ibge
    from tbl_municipios m
    inner join tbl_estados e on e.id_ibge = m.estado_id
    where e.sigla = 'SP' order by 3 asc;""")
    
    cities = cur.fetchall()
    
    cur.close()
    conn.close()

def main():
    loadCities()
    years = [2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023]
    # years = [1,2,3,4,5,6,7,8]
    for y in years:
        print(y)
        df = pd.read_csv(f'C:\\Users\\mathe\\Documents\\estudos\\engenharia_de_dados\\gov\\raw_data_incendios\\focos_br_sp_ref_{y}\\focos_br_sp_ref_{y}.csv')
        data = df.to_dict(orient='records')
        print(data[:1])
        # break
        ## {'latitude': 1.29117, 'longitude': -50.58522, 'data_pas': '2023-01-08 16:37:00', 'satelite': 'AQUA_M-T', 'pais': 'Brasil', 'estado': 'AMAPÁ', 'municipio': 'TARTARUGALZINHO', 'bioma': 'Amazônia', 'numero_dias_sem_chuva': 1.0, 'precipitacao': 0.2, 'risco_fogo': 0.09, 'id_area_industrial': 0, 'frp': 20.2}
        insertHistory(data[:int(len(data)/2)])
        print("insert")
        insertHistory(data[int(len(data)/2):])
        # break
    

if __name__ == '__main__':
    main()

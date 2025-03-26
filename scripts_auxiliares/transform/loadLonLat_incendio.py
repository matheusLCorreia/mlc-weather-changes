import requests as req
import psycopg2
import unidecode

def connectDatabase():
    try:
        with psycopg2.connect(f"""host=localhost dbname=weather_db port=5432 user=postgres password=18223005""") as conn:
            cur = conn.cursor()
            return conn, cur
    except (psycopg2.Error, psycopg2.DatabaseError) as pg:
        print(pg)
        exit(1)
    return None, None

def loadCities():
    conn, cur = connectDatabase()
    cur.execute("""select e.nome, e.sigla, m.nome, m.id_ibge
    from tbl_municipios m
    inner join tbl_estados e on e.id_ibge = m.estado_id
    where e.sigla = 'SP' order by 3 asc;""")
    
    data = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return data

def loadHist(): 
    conn, cur = connectDatabase()
    cur.execute("""select latitude, longitude, municipio_id
    from tbl_incendios_hist
    where municipio_id isnull and date_time >= '2003-01-01 00:00:00.000' and date_time <= '2003-12-31 23:59:59.000';""")
    
    data = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return data

def loadCityLonLat(lat, lon):
    # res = req.get(f"https://api.geoapify.com/v1/geocode/search?text={city_name}&apiKey=82931b68d4564b398344638fbb113676")
    res = req.get(f"https://api.geoapify.com/v1/geocode/reverse?lat={lat}&lon={lon}&apiKey=82931b68d4564b398344638fbb113676")
    print(res.status_code)
    
    data = res.json()
    city_name = ''
    if 'city' in data['features'][0]['properties'].keys():
        city_name = data['features'][0]['properties']['city']
        city_name = unidecode.unidecode(city_name).replace("'", "")
    
    print(city_name)
    return city_name
    
def main():
    cities = loadCities()
    data = loadHist()
    all_data = []
    conn, cur = connectDatabase()
    for h in data[:]:
        # print(h)
        city_name = loadCityLonLat(h[0], h[1])
        for city in cities:
            if city[2].lower() == city_name.lower():
                # print(city)
                all_data.append({'city_id': city[3], 'lon': h[1], 'lat': h[0]})
                cur.execute(f"UPDATE tbl_incendios_hist SET municipio_id = {city[3]} where longitude = {h[1]} and latitude = {h[0]} and extract(year from date_time) = 2003;")
                conn.commit()
                break

    print(all_data)
    
    # for row in all_data:
    #     # print(row)
    #     cur.execute(f"UPDATE tbl_incendios_hist SET municipio_id = {row['city_id']} where longitude = {row['lon']} and latitude = {row['lat']};")
    #     conn.commit()
        
    cur.close()
    conn.close()

# i = 0
# while i < 200:
#     main()
#     i = i + 1

main()
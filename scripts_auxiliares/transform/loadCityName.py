import requests as req
import psycopg2


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
    
def loadCityLonLat(city_name):
    res = req.get(f"https://api.geoapify.com/v1/geocode/search?text={city_name}&apiKey=82931b68d4564b398344638fbb113676")
    print(res.status_code)
    
    data = res.json()
    lon = data['features'][0]['properties']['lon']
    lat = data['features'][0]['properties']['lat']
    return lon, lat
    
def main():
    data = loadCities()
    all_data = []
    for city in data[:]:
        print(city)
        res = loadCityLonLat(city[2])
        all_data.append({'city_id': city[3], 'lon': res[0], 'lat': res[1]})

    # print(all_data)
    conn, cur = connectDatabase()
    for row in all_data:
        print(row)
        cur.execute(f"UPDATE tbl_municipios SET longitude = {row['lon']}, latitude = {row['lat']} where id_ibge = {row['city_id']};")
        conn.commit()
        
    cur.close()
    conn.close()
main()
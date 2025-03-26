import requests as req
import psycopg2
import datetime as dt
import json

years = [2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023]
# years = [2024]
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

def getData():
    conn, cur = connectDatabase()
    cur.execute("""select distinct
    --w._year, w._month,
    w._date,
    max(temperature_2m) over (partition by w._date) - min(temperature_2m) over (partition by w._date) amp_temp,
    --min(relative_humidity_2m) over (partition by w._date),
    min(relative_humidity_2m) over (partition by w._date) hum,
    max(precipitation) over (partition by w._date) - min(precipitation) over (partition by w._date) precp,
    max(rain) over (partition by w._date) _rain,
    --max(wind_speed_10m) over (partition by w._date),
    --max(soil_temperature_0_to_7cm) over (partition by w._date),
    --count(f.longitude) over (partition by w._year, w._week) fire_count,
    case when f.longitude isnull then 0
        else 1 end as fire
    from weather_rec w
    inner join tbl_municipios m on m.id_ibge = w.municipio_id
    left join tbl_incendios_hist f on f.municipio_id = m.id_ibge
        and extract(year from f.date_time) = w._year
        and extract(week from f.date_time) = w._week
    where lower(m.nome) in ('ribeirao preto') and w.date_time >= '2010-01-01 00:00:00.000 -0300' and w.date_time <= '2023-12-31 23:59:59.999 -0300'
    order by w._date asc;""")
    
    data = cur.fetchall()
    cur.close()
    conn.close()
    
    return data

def daysWithLowPrecipitation(data: dict):
    precp_min = 0
    count_days = 0
    for i in range(len(data)):
        if data[i]['amp_precip'] <= precp_min:
            data[i]['days_low_precp'] = count_days
            count_days = count_days + 1
        else:
            count_days = 0
            data[i]['days_low_precp'] = count_days
    
    return data
    
def daysWithLowRain(data: dict):
    rain_min = 0
    count_days = 0
    for i in range(len(data)):
        if data[i]['rain'] <= rain_min:
            data[i]['days_low_rain'] = count_days
            count_days = count_days + 1
        else:
            count_days = 0
            data[i]['days_low_rain'] = count_days
    
    return data
            
def init():
    records = getData()
    data = []
    i = 1
    for row in records:
        # print(row)
        # print(row[0].isocalendar())
        date_data = row[0].isocalendar()
        data.append({"index": i, "_date": str(row[0]), "_year": row[0].year, "_week": date_data[1], "_day_week": date_data[2],
                     "amp_temp": row[1], "humd": row[2], "amp_precip": row[3], "rain": row[4], "fire": row[5]})
        
        i = i + 1
    formatted_data = daysWithLowRain(daysWithLowPrecipitation(data))
    print(formatted_data)
    with open('./dados.json', 'w') as fl:
        fl.write(json.dumps(formatted_data))
        
init()
# import pathlib
import json
import requests as req
import datetime as dt

def loadEnvironment():
    # path = pathlib.Path().resolve()
    with open(f'/root/airflow/dags/environment.json', 'r') as fl:
        data = json.loads(fl.read())
        return data
    
def extractCityByCoordGoogle(lat, lon):
    env = loadEnvironment()
        
    params = env['GOOGLE']['urls']['params']
    __key = env['GOOGLE']['__key']
    
    url = f"{str(env['GOOGLE']['urls']['geocode'])}?{params}"
    url = url.replace("value2", f"{lat},{lon}").replace("value1", __key)
    
    res = req.get(url)
    data = res.json()
    
    location = data['results'][0]['address_components']
    city = ''
    state = ''
    for row in location:
        if 'administrative_area_level_2' in row['types']:
            city = row['short_name']
        elif 'administrative_area_level_1' in row['types']:
            state = row['short_name']
            
    return city, state

def formatDate(value: str):
    value = value.split('/')
    year = value[0]
    month = value[1]
    day = value[2]
    
    return f"{year}-{month}-{day}"

def getHour(value: str):
    value = value[:4]
    return f"{value[:2]}:{value[2:]}:00"

def formatTo(value):
    if len(value) == 0:
        return 'null'
    return float(value)

def getDateHourByEpoch(epoch):
    date_time = dt.datetime.fromtimestamp(epoch)
    date = date_time.strftime('%Y-%m-%d')
    time = date_time.strftime('%H:%M:%S')
    
    return date, time
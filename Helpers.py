# import pathlib
import json
import requests as req

def loadEnvironment():
    # path = pathlib.Path().resolve()
    with open(f'C:\\Users\\mathe\\Documents\\estudos\\engenharia_de_dados\\project_tcc_simple\\environment.json', 'r') as fl:
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

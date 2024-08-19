import Controller
import datetime as dt
    
def init():
    controller = Controller.Controller()
    cities = ['campinas', 'paulinia', 'valinhos']
    
    for city in cities:
        print(f"{dt.datetime.now()}; Extracting current weather data to {city}.")
        location, weather, air = controller.loadWeatherApiRealtime(city)
        controller.insertCurrentWeatherData({'location': location, 'weather': weather, 'air': air}, city)
    
init()
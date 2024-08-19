import Helpers as hlp
import requests as req

class WeatherAPI:
    __env = []
    __token = ''
    
    def __init__(self):
        print("WeatherAPI class...")
        self.__env = hlp.loadEnvironment()
        self.__url_base = f'{self.__env['WeatherAPI']['weather_api_base_path']}'
        self.__token = 'bf825594bacd485a9d733229240708'
    
    def loadRealtimeData(self, city):
        print("loadRealtimeData method..., ", city)
        try:
            res = req.get(url=f"{self.__url_base}/current.json?key={self.__token}&q={city}&aqi=yes")
            print(res.status_code, res)
            if res.status_code == 200:
                return res.json()
            else:
                print("Fail on loading data...")
                exit(2)
                
        except (req.ConnectionError, req.HTTPError, req.RequestException) as conn_htt_err:
            print("Fail on Weather API connection/request. ", conn_htt_err)
            exit(1)
        except (req.Timeout) as timeout_err:
            print("Timeout on Weather API request. ", timeout_err)
            exit(2)
                
import Controller
import datetime as dt

def init():
    controller = Controller.Controller()
    controller.extractFilesHistoryWeatherGov()
    
init()
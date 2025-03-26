dt = ['temperature_2m', 'relative_humidity_2m', 'apparent_temperature', 'surface_pressure', 'precipitation', 'rain', 'shortwave_radiation', 'direct_radiation', 'diffuse_radiation', 'wind_speed_10m', 'wind_gusts_10m', 'soil_temperature_0_to_7cm', 'soil_moisture_0_to_7cm']

arr = []
for r in dt:
    arr.append(f"{r}_max")
    arr.append(f"{r}_min")
    arr.append(f"{r}_avg")
    
print(arr)
import requests as r
import psycopg2
import unidecode
import pandas as pd


def connectDatabase():
    conn = psycopg2.connect("host=<secret> dbname=weather_db port=5432 user=postgres password=<secret>")
    cur = conn.cursor()
    
    return conn, cur

def insertHistory(data):
    conn, cur = connectDatabase()
    query = "INSERT INTO tbl_incendios_hist (longitude, latitude, date_time) VALUES "
    for row in data:
        query += f"({row['longitude']}, {row['latitude']}, '{row['data_pas']}'),"
        
    # print(query)
    # cur.execute("truncate ;")
    cur.execute(query[:len(query)-1])
    conn.commit()
    cur.close()
    conn.close()
    
def main():
    df = pd.read_csv('C:\\Users\\mathe\\Documents\\estudos\\engenharia_de_dados\\gov\\raw_data_gov\\focos_br_ref_2023\\focos_br_ref_2023.csv')
    data = df.to_dict(orient='records')
    ## {'latitude': 1.29117, 'longitude': -50.58522, 'data_pas': '2023-01-08 16:37:00', 'satelite': 'AQUA_M-T', 'pais': 'Brasil', 'estado': 'AMAPÁ', 'municipio': 'TARTARUGALZINHO', 'bioma': 'Amazônia', 'numero_dias_sem_chuva': 1.0, 'precipitacao': 0.2, 'risco_fogo': 0.09, 'id_area_industrial': 0, 'frp': 20.2}
    insertHistory(data)
    

if __name__ == '__main__':
    main()

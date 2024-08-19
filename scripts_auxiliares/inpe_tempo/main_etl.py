import requests as r
import xmltodict
import psycopg2
import unidecode


## Performar analises simples, como: 
# Identificação de zonas de incendio agrupado pelo tempo
# Fazer análise preditiva entre local, temperatura, umidade e ultimos incendios
url_path = 'http://servicos.cptec.inpe.br/XML/cidade/244/previsao.xml'

def connectDatabase():
    conn = psycopg2.connect("host=<secret> dbname=weather_db port=5432 user=postgres password=<secret>")
    cur = conn.cursor()
    
    return conn, cur

def getCidadesIBGE():
    res = r.get('https://servicodados.ibge.gov.br/api/v1/localidades/regioes-intermediarias/3510/municipios')
    raw_cities = res.json()
    data = []
    for row in raw_cities:
        data.append({"id": row['id'], 'nome': unidecode.unidecode(row['nome']).replace("'", "")})
        
    return data

def getCidadesInpe(cidade):
    res = r.get(f'http://servicos.cptec.inpe.br/XML/listaCidades?city={cidade}')
    data = xmltodict.parse(res.content)
    data_formatted = []
    # print(data['cidades']['cidade'])
    if type(data['cidades']['cidade']) == dict:
        data_formatted.append(data['cidades']['cidade'])
    else:
        for row in data['cidades']['cidade']:
            # print(row)
            data_formatted.append(row)
        
    return data_formatted
        
def insertCidades(cidades):
    conn, cur = connectDatabase()
    query = "INSERT INTO cities (id, nome, uf) VALUES "
    for cid in cidades:
        query += f"({cid['id']}, '{cid['nome']}', '{cid['uf']}'),"
        
    print(query)
    # cur.execute("truncate cities;")
    cur.execute(query[:len(query)-1])
    conn.commit()
    cur.close()
    conn.close()
    
    
def insertPrevisao4(data, cid):
    conn, cur = connectDatabase()
    query = "INSERT INTO previsao_4dias (date_time, tempo, maxima, minima, iuv, cidade_id) VALUES "
    for row in data:
        query += f"('{row['dia']}', '{row['tempo']}', {row['maxima']}, {row['minima']}, {row['iuv']}, {cid}),"
        
    print(query)
    cur.execute(query[:len(query)-1])
    conn.commit()
    cur.close()
    conn.close()
    
def loadPrevisaoTempo4Dias(cidade_id):
    res = r.get(f'http://servicos.cptec.inpe.br/XML/cidade/7dias/{cidade_id}/previsao.xml')
    data = xmltodict.parse(res.content)
    data_formatted = []
    return data

def loadCidades():
    conn, cur = connectDatabase()
    cur.execute("select id from cities;")
    data = cur.fetchall()
    cur.close()
    conn.close()
    
    return data

def main():
    cidades = getCidadesIBGE()
    for row in cidades:
        # print(row)
        insertCidades(getCidadesInpe(row['nome'].lower()))
    # exit()
    cidades = loadCidades()
    for c in cidades:
        print(c)
        previsao = loadPrevisaoTempo4Dias(c[0])
        insertPrevisao4(previsao['cidade']['previsao'], c[0])
        # break

if __name__ == '__main__':
    main()

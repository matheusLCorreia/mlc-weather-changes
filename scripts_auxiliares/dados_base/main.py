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

def getRegioesIBGE():
    res = r.get('https://servicodados.ibge.gov.br/api/v1/localidades/regioes')
    raw_cities = res.json()
    data = []
    for row in raw_cities:
        data.append({"id": row['id'], 'nome': unidecode.unidecode(row['nome']).replace("'", ""), "sigla": row['sigla']})
        
    return data

def getEstadosIBGE():
    res = r.get('https://servicodados.ibge.gov.br/api/v1/localidades/estados')
    raw_estados = res.json()
    data = []
    for row in raw_estados:
        data.append({"id": row['id'], 'nome': unidecode.unidecode(row['nome']).replace("'", ""), "sigla": row['sigla'], "regiao_id": row['regiao']['id']})
        
    return data

def getRegioesMetrop(estado):
    res = r.get(f'https://servicodados.ibge.gov.br/api/v1/localidades/estados/{estado}/regioes-metropolitanas')
    raw = res.json()
    regioes_metrop = []
    municipios = []
    for row in raw:
        # print(row)
        regioes_metrop.append({"id": row['id'], 'nome': unidecode.unidecode(row['nome']).replace("'", ""), "estado_id": row['UF']['id']})
        for cidade in row['municipios']:
            municipios.append({"id": cidade['id'], 'nome': unidecode.unidecode(cidade['nome']).replace("'", ""), "regiao_metrop_id": row['id']})
    
    return regioes_metrop, municipios
        
def insertRegioes(regioes):
    conn, cur = connectDatabase()
    query = "INSERT INTO tbl_regioes (id_ibge, sigla, nome) VALUES "
    for cid in regioes:
        query += f"({cid['id']}, '{cid['sigla']}', '{cid['nome']}'),"
        
    print(query)
    # cur.execute("truncate ;")
    cur.execute(query[:len(query)-1])
    conn.commit()
    cur.close()
    conn.close()
    
def insertEstados(estados):
    conn, cur = connectDatabase()
    query = "INSERT INTO tbl_estados (id_ibge, sigla, nome, regiao_id) VALUES "
    for cid in estados:
        query += f"({cid['id']}, '{cid['sigla']}', '{cid['nome']}', {cid['regiao_id']}),"
        
    print(query)
    # cur.execute("truncate ;")
    cur.execute(query[:len(query)-1])
    conn.commit()
    cur.close()
    conn.close()
    
def insertRegiaoMetrop(regioes):
    conn, cur = connectDatabase()
    query = "INSERT INTO tbl_regiao_metrop (id_ibge, nome, id_ibge_estado) VALUES "
    for cid in regioes:
        query += f"('{cid['id']}', '{cid['nome']}', {cid['estado_id']}),"
        
    # print(query)
    # cur.execute("truncate ;")
    cur.execute(query[:len(query)-1])
    conn.commit()
    cur.close()
    conn.close()
    
def insertMunicipios(cidades):
    conn, cur = connectDatabase()
    query = "INSERT INTO tbl_municipios (id_ibge, nome, regiao_metrop_id) VALUES "
    for cid in cidades:
        query += f"({cid['id']}, '{cid['nome']}', '{cid['regiao_metrop_id']}'),"
        
    # print(query)
    # cur.execute("truncate ;")
    cur.execute(query[:len(query)-1])
    conn.commit()
    cur.close()
    conn.close()
    
def main():
    # insertRegioes(getRegioesIBGE())
    # insertEstados(getEstadosIBGE())
    estados = getEstadosIBGE()
    for est in estados:
        print(est)
        regiao_m, municipios = getRegioesMetrop(est['sigla'])
        if len(regiao_m) > 0:
            insertRegiaoMetrop(regiao_m)
        if len(municipios) > 0:
            insertMunicipios(municipios)
        # break

if __name__ == '__main__':
    main()

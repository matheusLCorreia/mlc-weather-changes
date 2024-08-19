import json

def loadConfig():
    data = []
    with open('C:\\Users\\mathe\\Documents\\estudos\\engenharia_de_dados\\project_tcc_simple\\environment.json', 'r') as cfg:
        data = json.loads(cfg.read())
        
    return data['AWS_S3']['access_key'], data['AWS_S3']['secret_access_key']
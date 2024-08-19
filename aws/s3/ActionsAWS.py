import AWSController as aws

def init():
    """How to
    
    https://cicerojmm.medium.com/gerenciamento-de-dados-no-amazon-s3-com-a-utiliza%C3%A7%C3%A3o-de-python-8450e6b988f9
    """
    aws_obj = aws.AWSController()
    # aws_obj.createBucket('xeroque-roumis-01')
    
    files = [
        'C:\\Users\\mathe\\Documents\\estudos\\engenharia_de_dados\\gov\\raw_data_gov\\focos_br_ref_2022\\focos_br_ref_2022.csv',
        'C:\\Users\\mathe\\Documents\\estudos\\engenharia_de_dados\\gov\\raw_data_gov\\focos_br_ref_2023\\focos_br_ref_2023.csv'
    ]
    
    # aws_obj.uploadObject(files[0], 'weather-raw-data-tcc', 'focos_incendios_2022.csv')
    # aws_obj.uploadObject(files[1], 'weather-raw-data-tcc', 'focos_incendios_2023.csv')
    
    # aws_obj.getBuckets()
    
    # aws_obj.getObjectsFromBucket('weather-raw-data-tcc')
    
    # aws_obj.downloadObject('weather-raw-data-tcc', 'focos_incendios_2022.csv', 'focos_incendios_2022.csv')
    
if __name__ == '__main__':
    init()
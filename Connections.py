import psycopg2
import Helpers

class DW:
    def __init__(self):
        print("")
        # return self.connectDatabase()
        
    def connectDatabase(self):
        try:
            env = Helpers.loadEnvironment()
            # conn = 
            with psycopg2.connect(f"""host={env['PostgresDW']['ip_dns']} 
                                    dbname={env['PostgresDW']['database']} 
                                    port={env['PostgresDW']['port']} 
                                    user={env['PostgresDW']['user']} 
                                    password={env['PostgresDW']['password']}""") as conn:
                cur = conn.cursor()
            
                return conn, cur
        except (psycopg2.Error, psycopg2.DatabaseError) as pg:
            print(pg)
            exit(1)
        return None, None

# def loadLocations():
#     conn, cur = connectDatabase()

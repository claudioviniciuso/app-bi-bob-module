import psycopg2 as db
import os
import json
import logging


class config_db:
    # Iniciar Classe
    def __init__(self):
        self.status_file_config = None
        self.status_connection = False
        self.host = None
        self.database = None
        self.username = None
        self.password = None
        self.cnn = None
        self.cursor = None

        self.log = logging.getLogger(__name__)
        self.log.debug('Classe config_db iniciada com todas as variáveis nulas.')
    
    # Localizar arquivo Configuração
    def get_db_config(self):
        if os.path.isfile('app/db_config.json') == True:
            with open('app/db_config.json', 'r') as config:
                    data = json.loads(config.read())
                    self.host = data['host']
                    self.database = data['database']
                    self.username = data['username']
                    self.password = data['pwd']
                    self.port = data['port']
            if self.host == "" or self.database == "" or self.username == "" or self.password == "":
                self.status_file_config = False
            else:
                self.status_file_config = True
            config.close()
        return data

    def post_db_config(self,host,database,username,port):
        with open('app/db_config.json', 'w') as config:
            config.write('{"host":"' + host + ' ","database":"' + database + '","username":"' + username + '","port":"' + port + '"}')
            config.close()
    

    def connect_to_db(self):
        if self.status_file_config == True:
            try: 
                self.cnn = db.connect(host=self.host, 
                                        database=self.database,
                                        user=self.username, 
                                        password=self.password
                                        )
                self.status_connection = True
            except:
                self.status_connection = False
        
        return self.status_connection     

    def exec(self):
        self.get_db_config()
        self.connect_to_db()

        stats_db = {
            "status_file_config": self.status_file_config,
            "status_connection": self.status_connection,
            "host": self.host,
            "database": self.database,
            "username": self.username,
            "password": self.password,
            "port": self.port
        }

        return stats_db

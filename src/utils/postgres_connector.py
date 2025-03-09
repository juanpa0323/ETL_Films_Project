import psycopg2
from utils.Logger import Logger  

class PostgresConnector:
    def __init__(self, db_name='films_database', user='admin', password='admin', host='postgres', port='5432'):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
    
    def connect(self):
        """Establece la conexión con la base de datos PostgreSQL."""
        Logger.add_to_log('info', f'Conectando a la base de datos......')
        try:
            self.conn = psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            Logger.add_to_log('info', f'Conexión exitosa a la base de datos {self.db_name}')
            return self.conn
        except Exception as e:
            Logger.add_to_log('error', f'Error al conectar a la base de datos: {e}')
            return None

    def close_connection(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            Logger.add_to_log('info', 'Conexión a la base de datos cerrada.')


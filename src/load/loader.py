import psycopg2
from psycopg2 import sql, ProgrammingError
from utils.postgres_connector import PostgresConnector
from utils.Logger import Logger  
from pyspark.sql import DataFrame
class Loader:
    def __init__(self):
        self.connector = PostgresConnector()
        self.connector.connect()
        self.conn = self.connector.conn
    

    def create_tables_db(self, table_definitions):
        Logger.add_to_log('info', 'Creando tabals en la BD....')
        if not self.conn or self.conn.closed:
            Logger.add_to_log('error', 'Conexión a la base de datos no establecida.')
            return

        try:
            with self.conn.cursor() as cursor:
                for table, columns in table_definitions.items():
                    query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
                        sql.Identifier(table),
                        sql.SQL(", ").join(map(sql.SQL, columns))
                    )
                    cursor.execute(query)
                    Logger.add_to_log('info', f'Tabla "{table}" creada correctamente.')
            self.conn.commit()

        except ProgrammingError as e:
            Logger.add_to_log('error', f'Error en la consulta SQL: {e}')
        except Exception as e:
            Logger.add_to_log('error', f'Error al crear las tablas: {e}')

    def insert_data_db(self, table_name, data):
        if not self.conn or self.conn.closed:
            Logger.add_to_log('error', 'Conexión a la base de datos no establecida.')
            return

        if not data:
            Logger.add_to_log('warning', f'No hay datos para insertar en la tabla {table_name}.')
            return

        try:
            with self.conn.cursor() as cursor:
                columns = data[0].keys()
                query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({values});").format(
                    table=sql.Identifier(table_name),
                    fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
                    values=sql.SQL(', ').join(sql.Placeholder() * len(columns))
                )

                # Optimización en la transformación de datos
                records = [
                    tuple(None if isinstance(value, str) and value.strip().upper() == 'NULL' else value  
                        for value in row.values())  
                    for row in data
                ]

                
                cursor.executemany(query, records)
                self.conn.commit()
                Logger.add_to_log('info', f'Se insertaron {len(data)} registros en la tabla {table_name}')
        except ProgrammingError as e:
            self.conn.rollback()
            Logger.add_to_log('error', f'Error en la consulta SQL durante la inserción: {e}')
        except Exception as e:
            self.conn.rollback()
            Logger.add_to_log('error', f'Error al insertar datos en {table_name}: {e}')
   

    def safe_insert_db(self, table_name, df):
        try:
            self.insert_data_db(table_name, df)
            Logger.add_to_log('info', f'Datos cargados en la tabla {table_name} correctamente.')
        except Exception as ex:
            Logger.add_to_log('error', f'Error al cargar la tabla {table_name}: {str(ex)}')

    def close_connection_db(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            Logger.add_to_log('info', 'Conexión a la base de datos cerrada.')        
        
    def save_as_parquet(self, df: DataFrame, path: str):
        try:
            df.write.mode("overwrite").parquet(path)
            Logger.add_to_log('info', f"Archivo Parquet guardado exitosamente en: {path} con {df.count()} registros.")
        except Exception as ex:
            Logger.add_to_log('error', f"Error al guardar el archivo Parquet en {path}: {str(ex)}")
        

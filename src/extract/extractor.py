import pandas as pd
import traceback
from pyspark.sql import SparkSession
from utils.Logger import Logger

class DataExtractor:
    def __init__(self, file_path: str, spark: SparkSession):
        self.file_path = file_path
        self.spark = spark
        Logger.add_to_log('info', f'Inicializando DataExtractor con el archivo: {file_path}')

    def read_sheet(self, sheet_name: str):
        """
        Lee una hoja específica del archivo Excel y la convierte en un DataFrame de Spark.
        """
        Logger.add_to_log('info', f'Leyendo la hoja "{sheet_name}"...')
        try:
            # Utilizamos pandas para leer la hoja del Excel
            pdf = pd.read_excel(self.file_path, sheet_name=sheet_name)
            # Convertimos el DataFrame de pandas a DataFrame de Spark
            df = self.spark.createDataFrame(pdf)
            count = df.count()  # Contamos los registros
            Logger.add_to_log('info', f'Hoja "{sheet_name}" cargada con {count} registros.')
            return df
        except Exception as ex:
            Logger.add_to_log('error', f'Error al leer la hoja "{sheet_name}": {str(ex)}')
            Logger.add_to_log('error', traceback.format_exc())
            return None

    def extract_all(self):
        """
        Extrae todas las hojas de interés y retorna un diccionario con el nombre de la hoja y su DataFrame.
        """
        
        sheets = ['store','customer','rental','inventory','film']
        
        dataframes = {}
        for sheet in sheets:
            df = self.read_sheet(sheet)
            if df is not None:
                dataframes[sheet] = df
            else:
                Logger.add_to_log('warn', f'La hoja "{sheet}" no se pudo cargar.')
        return dataframes

from pyspark.sql import DataFrame
from transform.generic_transformer import GenericTransformer
from utils.Logger import Logger
import traceback
class CustomerTransformer(GenericTransformer):
    def __init__(self):
        super().__init__()

    def transform_customer(self, df: DataFrame) -> DataFrame:
        Logger.add_to_log('info', "Tranformando datos de la tabla customers")
        try:
            # Eliminar espacios en blanco en los nombres de las columnas
            df = self.trim_column_names(df)

            # Convertir las columnas de ID a enteros, descartando valores no convertibles
            df = self.convert_to_int(df, 'customer_id')
            df = self.convert_to_int(df, 'store_id')
            df = self.convert_to_int(df, 'address_id')

            # Eliminar espacios en blanco en los valores de las columnas clave
            df = self.trim_column_values(df, 'customer_id_old')
            df = self.trim_column_values(df, 'first_name')
            df = self.trim_column_values(df, 'last_name')
            df = self.trim_column_values(df, 'email')

            # Formatear columnas de fecha a formato "yyyy-MM-dd HH:mm:ss"
            df = self.format_timestamp_column(df, "create_date", "yyyy-MM-dd HH:mm:ss")
            df = self.format_timestamp_column(df, "last_update", "yyyy-MM-dd HH:mm:ss")

            # Eliminar filas duplicadas
            df = self.remove_duplicates(df)
            
            Logger.add_to_log('info', 'Datos procesados correctamente')

            return df
        except Exception as ex:
            Logger.add_to_log('error', f'Error al transformar datos de la tabal "store" {str(ex)}')
            Logger.add_to_log('error', traceback.format_exc())
    

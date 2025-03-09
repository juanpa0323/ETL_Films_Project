from pyspark.sql import DataFrame
from transform.generic_transformer import GenericTransformer
from utils.Logger import Logger
import traceback
class RentalTransformer(GenericTransformer):
    def __init__(self):
        super().__init__()

    def transform_rental(self, df: DataFrame) -> DataFrame:
        Logger.add_to_log('info', 'Transformado deatos de la tabla "rental"')

        try:    
            # Eliminar espacios en blanco en los nombres de las columnas
            df = self.trim_column_names(df)
            #Validar columnas requeridas
            required_columns = ['rental_id', 'rental_date', 'inventory_id', 'customer_id', 'return_date', 'staff_id', 'last_update']
            df = self.validate_columns(df, required_columns)       

            # Convertir columnas de ID a enteros, descartando valores no convertibles
            df = self.convert_to_int(df, 'rental_id')
            df = self.convert_to_int(df, 'inventory_id')
            df = self.convert_to_int(df, 'customer_id')

            # Formatear columnas de fecha
            df = self.format_timestamp_column(df, "rental_date", "yyyy-MM-dd HH:mm:ss")
            df = self.format_timestamp_column(df, "return_date", "yyyy-MM-dd HH:mm:ss")
            df = self.format_timestamp_column(df, "last_update", "yyyy-MM-dd HH:mm:ss")
            # Eliminar filas duplicadas
            df = self.remove_duplicates(df)

            Logger.add_to_log('info', 'Datos procesados correctamente')

            return df
        except Exception as ex:
            Logger.add_to_log('error', f'Error al transformar datos de la tabal "store" {str(ex)}')
            Logger.add_to_log('error', traceback.format_exc())
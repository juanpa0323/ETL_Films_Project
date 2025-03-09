from pyspark.sql import DataFrame
from transform.generic_transformer import GenericTransformer
from utils.Logger import Logger
import traceback
class InventoryTransformer(GenericTransformer):
    def __init__(self):
        super().__init__()

    def transform_inventory(self, df: DataFrame) -> DataFrame:
      
        try:
            # Eliminar espacios en blanco en los nombres de las columnas
            df = self.trim_column_names(df)
            #Validar columnas requeridas
            required_columns = ['inventory_id', 'film_id', 'store_id', 'last_update']
            df = self.validate_columns(df, required_columns)

            df = self.handle_null_values(df, ['store_id', 'manager_staff_id', 'address_id'])
            # Convertir columnas de ID a enteros, descartando valores no convertibles
            df = self.convert_to_int(df, 'inventory_id')
            df = self.convert_to_int(df, 'film_id')
            df = self.convert_to_int(df, 'store_id')

            # Formatear columna de fecha
            df = self.format_timestamp_column(df, "last_update", "yyyy-MM-dd HH:mm:ss")
            # Eliminar filas duplicadas
            df = self.remove_duplicates(df)

            Logger.add_to_log('info', 'Datos procesados correctamente')

            return df
        except Exception as ex:
            Logger.add_to_log('error', f'Error al transformar datos de la tabal "store" {str(ex)}')
            Logger.add_to_log('error', traceback.format_exc())

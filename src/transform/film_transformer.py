from pyspark.sql import DataFrame
from transform.generic_transformer import GenericTransformer
from utils.Logger import Logger
import traceback
class FilmTransformer(GenericTransformer):
    def __init__(self):
        super().__init__()

    def transform_film(self, df: DataFrame) -> DataFrame:
        Logger.add_to_log('info', 'Ternsformando datos de la tabla film')
        try:
            # Eliminar espacios en blanco en los nombres de las columnas
            df = self.trim_column_names(df)

            #Validar columnas requeridas
            required_columns = ['film_id', 'title', 'description', 'release_year','language_id','original_language_id',
                                'rental_duration', 'rental_rate','length','replacement_cost','num_voted_users',
                                'rating', 'special_features','last_update'
                                ]
            df = self.validate_columns(df, required_columns)        

            # Convertir columnas de ID a enteros, descartando valores no convertibles
            df = self.convert_to_int(df, 'film_id')
            
            # Eliminar espacios en blanco en los valores de columnas clave
            df = self.trim_column_values(df, 'title')
            df = self.trim_column_values(df, 'description')

            # Convertir y validar columnas numéricas
            df = self.convert_to_int(df, 'release_year')
            df = self.convert_to_int(df, 'language_id')
            df = self.convert_to_int(df, "rental_duration")         
            df = self.validate_positive(df, "rental_duration")            

          
            df = self.convert_to_float(df, "rental_rate")
            df = self.validate_positive(df, "rental_rate")

            
            df = self.convert_to_int(df, "length")
            df = self.validate_positive(df, "length")

            df = self.convert_to_float(df, "replacement_cost")

            df = self.convert_to_int(df, "num_voted_users")
            df = self.validate_positive(df, "num_voted_users")

            df = self.trim_column_values(df, 'rating')
            df = self.trim_column_values(df, 'special_features')

            # Formatear columna de fecha
            df = self.format_timestamp_column(df, "last_update", "yyyy-MM-dd HH:mm:ss")

            # Eliminar filas duplicadas
            df = self.remove_duplicates(df)

            Logger.add_to_log('info', 'Datos procesados correctamente')

            return df
        except Exception as ex:
            Logger.add_to_log('error', f'Error al transformar datos de la tabal "store" {str(ex)}')
            Logger.add_to_log('error', traceback.format_exc())

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, trim,to_timestamp
from utils.Logger import Logger
class GenericTransformer:
    def __init__(self):
        pass

    def convert_to_int(self, df: DataFrame, column_name: str) -> DataFrame:
      """
      Convierte la columna 'column_name' a entero y descarta filas donde no se pueda convertir.
      Retorna el DataFrame resultante.
      """
      df = self.trim_column_values(df, column_name)
      df = df.withColumn(column_name + "_tmp", col(column_name).cast("integer"))
      df = df.filter(col(column_name + "_tmp").isNotNull())
      df = df.drop(column_name).withColumnRenamed(column_name + "_tmp", column_name)
      return df

    def convert_to_float(self, df: DataFrame, column_name: str) -> DataFrame:
      """
      Convierte la columna 'column_name' a float/double y descarta filas donde no se pueda convertir.
      Retorna el DataFrame resultante.
      """
      df = self.trim_column_values(df, column_name)
      df = df.withColumn(column_name + "_tmp", col(column_name).cast("double"))
      df = df.filter(col(column_name + "_tmp").isNotNull())
      df = df.drop(column_name).withColumnRenamed(column_name + "_tmp", column_name)
      return df
    
    def validate_positive(self, df: DataFrame, column_name: str) -> DataFrame:
      """
      Filtra el DataFrame para conservar únicamente las filas donde el valor de 'column_name' es mayor a 0.
      
      Parámetros:
        - df: DataFrame de entrada.
        - column_name: Nombre de la columna a validar.
      
      Retorna:
        - DataFrame filtrado, que contiene solo filas con valores positivos en la columna indicada.
      """
      return df.filter(col(column_name) >= 0)
    

    def remove_duplicates(self, df: DataFrame) -> DataFrame:
      """
      Elimina filas duplicadas basándose en todas las columnas del DataFrame.
      """
      return df.dropDuplicates()

    def trim_column_names(self, df: DataFrame) -> DataFrame:
      """
      Renombra todas las columnas del DataFrame eliminando los espacios al inicio y al final.
      """
      for col_name in df.columns:
          trimmed_name = col_name.strip()
          if col_name != trimmed_name:
              df = df.withColumnRenamed(col_name, trimmed_name)
      return df

    def trim_column_values(self, df: DataFrame, column_name: str) -> DataFrame:
      """
      Elimina los espacios al inicio y al final de los valores de la columna 'column_name'.
      
      Parámetros:
        - df: DataFrame de entrada.
        - column_name: Nombre de la columna a limpiar.
      
      Retorna:
        - DataFrame con la columna 'column_name' actualizada (sin espacios al inicio ni al final).
      """
      # Se crea una columna temporal con el valor recortado
      trimmed_col = column_name + "_trimmed"
      df = df.withColumn(trimmed_col, trim(col(column_name)))
      # Se reemplaza la columna original por la versión recortada
      df = df.drop(column_name).withColumnRenamed(trimmed_col, column_name)
      return df

    def format_timestamp_column(self, df: DataFrame, column_name: str, format_str: str) -> DataFrame:
      """
      Convierte la columna 'column_name' (ya sin espacios) a timestamp usando el formato especificado.
      Filtra las filas que no se puedan convertir.
      
      Parámetros:
        - df: DataFrame de entrada.
        - column_name: Nombre de la columna a convertir.
        - format_str: Formato de la fecha, por ejemplo "yyyy-MM-dd HH:mm:ss".
      
      Retorna:
        - DataFrame con la columna 'column_name' convertida a timestamp y sin filas inválidas.
      """
      df = self.trim_column_values(df, column_name)
      ts_col = column_name + "_ts"
      # Se intenta convertir la columna ya limpia a timestamp
      df = df.withColumn(ts_col, to_timestamp(col(column_name), format_str))
      # Se filtran las filas en las que la conversión falla (resulta en null)
      df = df.filter(col(ts_col).isNotNull())
      # Se reemplaza la columna original por la versión convertida
      df = df.drop(column_name).withColumnRenamed(ts_col, column_name)
      return df
    
    def validate_columns(self, df: DataFrame, required_columns: list) -> DataFrame:
      """
      Verifica que el DataFrame tenga las columnas requeridas.
      Si faltan columnas críticas, se registra el error, se devuelve un DataFrame vacío,
      pero el programa continúa funcionando.

      Args:
          df (DataFrame): DataFrame de entrada.
          required_columns (list): Lista de columnas requeridas.

      Returns:
          DataFrame: DataFrame vacío si faltan columnas críticas.
      """
      missing_columns = [col for col in required_columns if col not in df.columns]
      
      if missing_columns:
          from utils.Logger import Logger
          Logger.add_to_log('error', f"Faltan las siguientes columnas requeridas: {missing_columns}")
          
          # Crear un DataFrame vacío con el mismo esquema que el original
          return df.limit(0)
      
      return df

    def handle_null_values(self, df: DataFrame, columns: list, fill_value=0) -> DataFrame:
      """
      Rellena los valores nulos en las columnas especificadas con el valor proporcionado.

      Args:
          df (DataFrame): DataFrame de entrada.
          columns (list): Lista de columnas en las que se deben manejar los nulos.
          fill_value (any, opcional): Valor con el que se llenarán los nulos. Por defecto es 0.

      Returns:
          DataFrame: DataFrame con los nulos manejados.
      """
      if not isinstance(df, DataFrame):  
          raise TypeError("El parámetro 'df' debe ser un DataFrame de PySpark.")
          
      for col in columns:
          if col in df.columns:  # Solo manejar columnas que realmente existen
              df = df.fillna({col: fill_value})
          else:
              from utils.Logger import Logger
              Logger.add_to_log('warning', f"⚠️ La columna '{col}' no existe en el DataFrame. No se procesaron nulos en ella.")
              
      return df


    

    

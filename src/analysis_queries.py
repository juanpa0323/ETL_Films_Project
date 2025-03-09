from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.Logger import Logger
class FilmInsights:
    def __init__(self, spark, df):
        self.spark = spark
        self.df = df

    def top_5_special_features(self):
        try:
            df = (self.df.groupBy('special_features')
                      .count()
                      .orderBy(F.desc('count'))
                      .limit(5))
            return [row['special_features'] for row in df.collect()]
        except Exception as ex:
            Logger.add_to_log('error', f'Error durante la carga de datos: {str(ex)}')

    def top_10_expensive_movies(self):
        try:
            df = (self.df.select('title', 'rental_rate', 'special_features')
                        .orderBy(F.desc('rental_rate'))
                        .limit(10))
            return [row.asDict() for row in df.collect()]
        except Exception as ex:
            Logger.add_to_log('error', f'Error durante la consulta de películas caras: {str(ex)}')

    def rental_rate_by_year(self):
        try:
            df = (self.df.groupBy('release_year')
                        .agg(F.avg('rental_rate').alias('avg_rental_rate'))
                        .orderBy('release_year'))
            return [row.asDict() for row in df.collect()]
        except Exception as ex:
            Logger.add_to_log('error', f'Error durante el cálculo de tarifa por año: {str(ex)}')

    def avg_rental_duration_by_special_features(self):
        try:
            df = (self.df.groupBy('special_features')
                        .agg(F.avg('rental_duration').alias('avg_rental_duration'))
                        .orderBy(F.desc('avg_rental_duration')))
            return [row.asDict() for row in df.collect()]
        except Exception as ex:
            Logger.add_to_log('error', f'Error durante el cálculo de duración promedio: {str(ex)}')

    
    def top_customers_by_rentals(self, rental_df, inventory_df, customer_df):
        try:
            customers_df = (rental_df
                        .join(inventory_df, "inventory_id", "inner")
                        .join(customer_df, "customer_id", "inner")
                        .groupBy("customer_id", "first_name", "last_name")
                        .count()
                        .withColumnRenamed("count", "total")
                        .orderBy(F.desc("total"))
                        .limit(5))  

            return [row.asDict() for row in customers_df.collect()]
        except Exception as ex:
            Logger.add_to_log('error', f'Error durante la consulta del cliente con más alquileres: {str(ex)}')
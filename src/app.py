from pyspark.sql import SparkSession
import traceback
from utils.Logger import Logger
from utils.metrics import start_metrics_server, records_processed, failed_records, measure_time
import time
from extract.extractor import DataExtractor
from transform.store_transformer import StoreTransformer
from transform.customer_tronsformer import CustomerTransformer
from transform.rental_transformer import RentalTransformer
from transform.inventory_transformer import InventoryTransformer
from transform.film_transformer import FilmTransformer
from load.loader import Loader
import json
from analysis_queries import FilmInsights
def main():
    start_time = time.time()  # Iniciar temporizador para medir el tiempo total
    start_metrics_server()    # Iniciar el servidor de métricas

    try:
        Logger.add_to_log('info', '<==================Iniciando la aplicación ETL...==================>')
        
        
        # Crear la sesión de Spark
        spark = SparkSession.builder \
            .appName("ETL_App") \
            .master("local[*]") \
            .getOrCreate()
        Logger.add_to_log('info', 'Sesión Spark creada con éxito.')

        # Instanciar el extractor y comenzar la extracción de datos
        extractor = DataExtractor("src/data/Films_2.xlsx", spark)
        Logger.add_to_log('info', 'Extractor instanciado. Iniciando extracción de datos...')

        # Extraer todas las hojas
        dataframes = extractor.extract_all()
        Logger.add_to_log('info', '<==================Extracción de datos completada=====================>')
        
        # Mostrar y registrar la cantidad de registros por cada hoja extraída
        for sheet, df in dataframes.items():
            count = df.count()
            records_processed.inc(count)  # Registrar registros procesados
            Logger.add_to_log('info', f'Hoja "{sheet}" contiene {count} registros después de la extracción.')
            print(f'Hoja "{sheet}" contiene {count} registros.')
        
        Logger.add_to_log('info', '<===============Asignando DataFrames a cada tabla========================>')      
                
        # Asignar DataFrames para cada tabla
        df_film = dataframes["film"]
        df_inventory = dataframes["inventory"]
        df_rental = dataframes["rental"]
        df_customer = dataframes["customer"]
        df_store = dataframes["store"]

        Logger.add_to_log('info', '<===============Tranformar datos de cada dataframe========================>')      

        # Transformación de la tabla Store
        store_transformer = StoreTransformer()
        df_store_clean = store_transformer.tranform_store(df_store)
        records_processed.inc(df_store_clean.count())  # Registrar registros transformados
        Logger.add_to_log('info', f'Hoja "store" transformada. Registros resultantes: {df_store_clean.count()}.')

        # Transformación de la tabla Customer
        customer_transformer = CustomerTransformer()
        df_customer_clean = customer_transformer.transform_customer(df_customer)
        records_processed.inc(df_customer_clean.count())
        Logger.add_to_log('info', f'Hoja "customer" transformada. Registros resultantes: {df_customer_clean.count()}.')

        # Transformación de la tabla Film
        film_transformer = FilmTransformer()
        df_film_clean = film_transformer.transform_film(df_film)
        records_processed.inc(df_film_clean.count())
        Logger.add_to_log('info', f'Hoja "film" transformada. Registros resultantes: {df_film_clean.count()}.')

        # Transformación de la tabla Inventory
        inventory_transformer = InventoryTransformer()
        df_inventory_clean = inventory_transformer.transform_inventory(df_inventory)
        records_processed.inc(df_inventory_clean.count())
        Logger.add_to_log('info', f'Hoja "inventory" transformada. Registros resultantes: {df_inventory_clean.count()}.')

        # Transformación de la tabla Rental
        rental_transformer = RentalTransformer()
        df_rental_clean = rental_transformer.transform_rental(df_rental)
        records_processed.inc(df_rental_clean.count())
        Logger.add_to_log('info', f'Hoja "rental" transformada. Registros resultantes: {df_rental_clean.count()}.')

        Logger.add_to_log('info', 'Todas las transformaciones completadas con éxito.')

        Logger.add_to_log('info', '<===============Guardar datos=======================>')
        try:
            # Crear instancia del conector
            loader = Loader()
            # Insertar datos en cada tabla           

            Logger.add_to_log('info', '<===============Guardando  base de datos postgres========================>')
            with open("src/utils/tables.json") as file:
                table_definitions = json.load(file)
            
            #print(tables)
            Logger.add_to_log('info', f"TAblas a crear en la base de datos: {list(table_definitions.keys())}")
            loader.create_tables_db(table_definitions)
            
        
            loader.safe_insert_db('store', df_store_clean.toPandas().to_dict(orient='records'))
            loader.safe_insert_db('customer', df_customer_clean.toPandas().to_dict(orient='records'))
            loader.safe_insert_db('film', df_film_clean.toPandas().to_dict(orient='records'))
            loader.safe_insert_db('inventory', df_inventory_clean.toPandas().to_dict(orient='records'))
            loader.safe_insert_db('rental', df_rental_clean.toPandas().to_dict(orient='records'))
            Logger.add_to_log('info', 'Datos cargados correctamente en la base de datos.')
              
            Logger.add_to_log('info', '<===============Guardar datos en archivos========================>')
            # Guardar como Parquet (en caso de necesitar archivos externos)
            loader.save_as_parquet(df_film_clean, "src/data/output/film_clean.parquet")
            loader.save_as_parquet(df_customer_clean, "src/data/output/customer_clean.parquet")
            loader.save_as_parquet(df_rental_clean, "src/data/output/rental_clean.parquet")
            loader.save_as_parquet(df_inventory_clean, "src/data/output/inventory_clean.parquet")
            loader.save_as_parquet(df_store_clean, "src/data/output/store_clean.parquet")             
            Logger.add_to_log('info', '<===============Archivos guardados correctamente========================>')
        except Exception as ex:
            Logger.add_to_log('error', f'Error durante la carga de datos: {str(ex)}')        
        finally:          
            loader.close_connection_db()

      

        Logger.add_to_log('info', '<===============Respondiendo a las preguntas========================>')
        insights = FilmInsights(spark, df_film_clean)
        Logger.add_to_log('info', "1. ->Top 5 special features con mayor número de registros:")
        Logger.add_to_log('info', str(insights.top_5_special_features()))

        Logger.add_to_log('info', "2. ->Total de alquileres por tienda:")
        Logger.add_to_log('info', str(insights.total_rental_by_store(df_rental_clean, df_inventory_clean,df_store_clean)))

        Logger.add_to_log('info', "3. ->Relación entre el año de lanzamiento y la tarifa de alquiler:")
        Logger.add_to_log('info', str(insights.rental_rate_by_year()))


        Logger.add_to_log('info', "4. ->Duración promedio de alquiler por special features:")
        Logger.add_to_log('info', str(insights.avg_rental_duration_by_special_features()))

        Logger.add_to_log('info', "5. ->Top 5 clientes con mayor número de alquileres realizados:")
        Logger.add_to_log('info', str(insights.top_customers_by_rentals(df_rental_clean, df_inventory_clean,df_customer_clean)))
        
        
    

    except Exception as ex:
        Logger.add_to_log('error', f'Error en la aplicación ETL: {str(ex)}')
        Logger.add_to_log('error', traceback.format_exc())
    
    finally:
        measure_time(start_time)  # Registrar tiempo total del ETL
        spark.stop()
        Logger.add_to_log('info', '<=======================Sesión Spark detenida. Aplicación ETL finalizada.=================>')

if __name__ == "__main__":
    main()

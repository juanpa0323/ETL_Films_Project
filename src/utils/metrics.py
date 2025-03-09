from prometheus_client import Counter, Gauge, start_http_server
import time

# ====== MÉTRICAS ======
records_processed = Counter('records_processed_total', 'Total de registros procesados')
processing_time = Gauge('etl_processing_time_seconds', 'Tiempo de ejecución del ETL')
failed_records = Counter('failed_records_total', 'Total de registros fallidos')

# Inicializar el servidor para exponer métricas
def start_metrics_server(host='0.0.0.0',port=8000):
    start_http_server(port)
    print(f"Servidor de métricas iniciado en el puerto {port}")

# Función para medir tiempo de ejecución
def measure_time(start_time):
    processing_time.set(time.time() - start_time)

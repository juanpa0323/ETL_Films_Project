# Proyecto ETL con PySpark y PostgreSQL

## Descripción del Proyecto

Este proyecto es una implementación de un pipeline ETL (Extract, Transform, Load) que utiliza **PySpark** para procesar datos provenientes de un archivo Excel y almacenarlos en una base de datos **PostgreSQL** dentro de un entorno Docker.

El pipeline está diseñado para ser escalable, modular y flexible, permitiendo agregar nuevas transformaciones o fuentes de datos con mínima refactorización.

---

## Estructura del Proyecto

├── src
│   ├── app.py
│   ├── extract
│   │   └── extractor.py
│   ├── transform
│   │   ├── generic_transformer.py
│   │   ├── store_transformer.py
│   │   ├── customer_transformer.py
│   │   ├── rental_transformer.py
│   │   ├── inventory_transformer.py
│   │   └── film_transformer.py
│   ├── load
│   │   └── loader.py
│   ├── utils
│   │   ├── logs
│   │   |   ├── app.log
│   │   ├── Logger.py
│   │   ├── metrics.py
│   │   ├── postgres_connector.py
│   │   └── tables.json
│   └── data
│       └── Films_2.xlsx
├── requirements.txt
├── README.md
├── docker-compose.yml
├── Dockerfile
└── .gitignore


---

## Instalación y Configuración

1. **Clona el repositorio:**
   - git clone https://github.com/juanpa0323/ETL_Films_Project.git
   - cd ETL_Films_Project


2. **Levanta los servicios usando Docker Compose:**
   - docker-compose up --build 
   - Nota: Se debe instalar Docker Desktop primero 


3. **Acceso a los servicios:**
 - La interfaz web de Spark está disponible en el puerto **4040**.
 - La base de datos PostgreSQL está expuesta en el puerto **5432**.

4. **Verifica que los datos se hayan cargado correctamente:**
Se pueden usar herramientas como **pgAdmin**, **DBeaver** o la consola de PostgreSQL para explorar la base de datos.



## Detalle de Implementación

### 1. **Extracción (`extractor.py`)**
- Extrae datos desde el archivo Excel `Films_2.xlsx`.
- Utiliza PySpark para leer el archivo y manejar grandes volúmenes de datos de forma eficiente.

### 2. **Transformación (Carpeta `transform/`)**
- Cada archivo maneja transformaciones específicas para diferentes conjuntos de datos.
- El archivo `generic_transformer.py` maneja:
  - Limpieza de nombres de columnas.
  - Validación de columnas requeridas.
  - Manejo de datos nulos.

### 3. **Carga (`loader.py`)**
  - Carga los datos transformados en PostgreSQL utilizando la API JDBC de PySpark.
  - Se implementa un sistema de manejo de errores para evitar interrupciones del flujo ETL.
  - Además, se genera una copia de los datos transformados en formato **Parquet**  en la carpeta `src/data/output`. Esto permite:
    - Realizar auditorías y revisiones de los datos limpios.
    - Agilizar el análisis sin necesidad de ejecutar nuevamente el flujo ETL completo.
    - Conservar un respaldo local en caso de fallos posteriores en la base de datos.

### 4. **Conexión con la Base de Datos (`postgres_connector.py`)**
  - Gestiona la conexión con PostgreSQL usando la librería `psycopg2`.
  - Permite reutilizar la conexión en otros módulos del proyecto.

---

## Justificación del Diseño

### 1. **Uso de PySpark**
 - PySpark permite manejar grandes volúmenes de datos de forma eficiente gracias a su procesamiento distribuido.

### 2. **Modularidad**
 - La separación en módulos (`extract`, `transform`, `load`) facilita la escalabilidad y el mantenimiento.

### 3. **Docker para Contenerización**
 - Garantiza que el entorno de desarrollo sea el mismo en cualquier sistema, evitando problemas de configuración.

### 4. **PostgreSQL para el Almacenamiento**
 - PostgreSQL es una base de datos robusta y ampliamente utilizada, ideal para manejar datos estructurados.

 

## Futuras Mejoras
  - Agregar pruebas unitarias para cada uno de los transformadores.
  - Implementar un dashboard para monitorear el rendimiento del pipeline ETL.
  - Optimizar las consultas SQL para mejorar la eficiencia del proceso de carga.

 



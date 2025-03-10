# Informe de Presentación de Resultados - Proyecto ETL FILMS

## 1. Arquitectura de Datos y Arquetipo de la Aplicación

### Arquitectura de Datos

El proyecto sigue una arquitectura ETL (Extract, Transform, Load) que permite la extracción de datos desde un archivo Excel, su transformación mediante PySpark y su carga en una base de datos PostgreSQL. La arquitectura está diseñada para garantizar escalabilidad, modularidad y facilidad de mantenimiento.

**Componentes principales:**

- **Extract:** Se encarga de la lectura de los datos desde el archivo `Films_2.xlsx`.
- **Transform:** Realiza la limpieza, validación y transformación de los datos mediante transformadores específicos y un transformador genérico para lógicas comunes.
- **Load:** Inserta los datos procesados en la base de datos PostgreSQL utilizando una conexión modularizada y, además, guarda los datos procesados en formato Prquet .
- **Utils:** Contiene funciones auxiliares para la gestión de logs, conexión con PostgreSQL y manejo de métricas.
- **Docker:** Se utiliza para facilitar la portabilidad y despliegue del entorno, utilizando un contenedor para PySpark y otro para PostgreSQL.

### Uso de Docker

El proyecto se ejecuta en un entorno Dockerizado que facilita la portabilidad y escalabilidad. Se implementaron dos contenedores principales:

- **Contenedor PySpark:** Encargado de ejecutar el pipeline ETL mediante el comando `spark-submit`. Utiliza el directorio `/app` como volumen compartido para el acceso a los datos y el código fuente.
- **Contenedor PostgreSQL:** Contiene la base de datos relacional donde se almacenan los datos procesados. Se utiliza un volumen (`filmsdata`) para persistir la información.

**Comando para levantar el entorno:**

```bash
docker-compose up --build
```

### Flujo del Proceso ETL

1. **Extracción:** Se lee el archivo `Films_2.xlsx`.
2. **Transformación:** Se limpian datos nulos, se renombran columnas y se verifican las columnas requeridas. Se utilizan transformadores específicos para cada tabla (`StoreTransformer`, `CustomerTransformer`, `RentalTransformer`, `InventoryTransformer`, `FilmTransformer`).
3. **Carga:** Los datos limpios se cargan en tablas dentro de PostgreSQL mediante el módulo `PostgresLoader`, que crea dinámicamente las tablas utilizando el archivo `tables.json`.

---

## 2. Análisis Exploratorio de Datos (EDA)

Durante el análisis de los datos se identificaron las siguientes características relevantes:

- **Cantidad de registros por hoja:**
  - `store`: 2 registros
  - `customer`: 1.392 registros
  - `film`: 1.003 registros (reducción a 977 después de transformación)
  - `inventory`: 4.581 registros (reducción a 4.571 después de transformación)
  - `rental`: 16.044 registros (reducción a 15.861 después de transformación)

### Problemas identificados en los datos

Durante la fase de exploración se encontraron las siguientes inconsistencias y errores:

- El archivo `Films_2.xlsx` contenía un espacio al final del nombre, lo que impedía su correcta lectura.
- En las hojas `customer`, `film`, `inventory`, `rental` y `store`, las columnas presentaban un espacio adicional al inicio de sus nombres.
- En la hoja `customer`, las columnas `customer_id` y `customer_id_old` no comparten el mismo tipo de dato.
- Se detectaron datos corruptos en claves foráneas de algunos registros, los cuales fueron corregidos durante la fase de transformación.

---

## 3. Preguntas de Negocio y Respuestas

Las siguientes preguntas se formularon para obtener insights relevantes a partir de los datos procesados:

### 1. ¿Cuáles son las 5 categorías de películas con mayor número de registros?
  - **Función** top_5_special_features()
  - Devuelve una lista con las 5 categorías más comunes en la columna special_features.
  - **Respuesta:**  `['Trailers', 'Commentaries', 'Deleted Scenes', 'Behind the Scenes']`

### 2. ¿Cuál es el total de alquires por tienda ?
  - **Función** total_rental_by_store()
  - Devuelve una lista de diccionarios con el id de cata tienda y el total de alquileres.
  - **Respuesta:**  `[{'store_id': 1, 'total': 12}, {'store_id': 2, 'total': 15816}]`

### 3. ¿Existe una relación entre el año de lanzamiento y la tarifa de alquiler?
  - **Función** rental_rate_by_year()
  - Devuelve una lista de diccionarios que muestra el promedio de la tarifa de alquiler por cada año de lanzamiento.
  - **Respuesta:** ` [{'release_year': 2006, 'avg_rental_rate': 2.9797645854656536}]`

###  4. ¿Cuál es la duración promedio de alquiler por categoría?
  - **Función** avg_rental_duration_by_special_features()
  - Devuelve una lista de diccionarios con la duración promedio de alquiler por special_features.
  - **Respuesta:** `[{'special_features': 'Behind the Scenes', 'avg_rental_duration': 5.25}, {'special_features': 'Deleted Scenes', 'avg_rental_duration': 5.0458015267175576}, {'special_features': 'Commentaries', 'avg_rental_duration': 5.031496062992126}, {'special_features': 'Trailers', 'avg_rental_duration': 4.912213740458015}]`

### 5. ¿Cuál es el cliente con mayor número de alquileres realizados?
  - **Función** top_customers_by_rentals()
  - Devuelve una lista de diccionarios  con los 5 clientes con mayor número de alquileres realizados
  - **Respuesta:**`[{'customer_id': 148, 'first_name': 'ELEANOR', 'last_name': 'HUNT', 'total': 46}, {'customer_id': 526, 'first_name': 'KARL', 'last_name': 'SEAL', 'total': 45}, {'customer_id': 144, 'first_name': 'CLARA', 'last_name': 'SHAW', 'total': 42}, {'customer_id': 236, 'first_name': 'MARCIA', 'last_name': 'DEAN', 'total': 41}, {'customer_id': 469, 'first_name': 'WESLEY', 'last_name': 'BULL', 'total': 40}]`

---

## 4. Conclusiones

- El pipeline ETL desarrollado logró limpiar y transformar los datos de forma efectiva, permitiendo su almacenamiento en una base de datos relacional para facilitar consultas rápidas.
- El uso de PySpark garantiza que el pipeline sea escalable y capaz de manejar grandes volúmenes de datos en el futuro.
- El diseño modular del proyecto permite agregar nuevas transformaciones o fuentes de datos con facilidad.
- El entorno Dockerizado facilita el despliegue en diferentes entornos y garantiza la reproducibilidad del proyecto.
- La implementación de métricas y logs brinda mayor control y visibilidad del proceso ETL, facilitando la detección y corrección de errores.

---

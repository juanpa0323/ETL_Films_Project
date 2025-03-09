FROM spark:3.5.5-scala2.12-java17-ubuntu

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    rm -rf /var/lib/apt/lists/*

# Copiar el archivo de dependencias y luego instalarlo
COPY requirements.txt /tmp/
RUN pip3 install --upgrade pip && pip3 install -r /tmp/requirements.txt

USER spark
WORKDIR /opt/spark/work-dir

# Copiar el código fuente al directorio de trabajo (ahora /opt/spark/work-dir)
COPY . /opt/spark/work-dir

# Configurar variables de entorno para PySpark 
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Asegurar que /opt/spark/bin esté en el PATH
ENV PATH="/opt/spark/bin:${PATH}"

# Ejecutar el script de prueba
CMD ["spark-submit", "src/app.py"]

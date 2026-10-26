 Tarea 3 – Procesamiento de datos con Apache Spark (Big Data, UNAD)

Curso Big Data (202016911) – Universidad Nacional Abierta y a Distancia.

Análisis en batch con PySpark del dataset **SECOP II – Contratos Electrónicos** (datos.gov.co).


¿Cómo se distribuyen los contratos públicos y el valor contratado por departamento,
tipo de contrato y modalidad de contratación?

- Fuente: https://www.datos.gov.co/resource/jbjy-vk9h
- 300.000 registros y 10 columnas: entidad, departamento, ciudad, estado del contrato,
  tipo de contrato, modalidad de contratación, valor del contrato, valor pagado,
  fecha de firma y proveedor adjudicado.


Requisitos: Hadoop (HDFS) y Spark instalados, Python 3.

1. Descargar los datos:

    wget 'https://www.datos.gov.co/resource/jbjy-vk9h.csv?$select=nombre_entidad,departamento,ciudad,estado_contrato,tipo_de_contrato,modalidad_de_contratacion,valor_del_contrato,valor_pagado,fecha_de_firma,proveedor_adjudicado&$limit=300000' -O contratos.csv

2. Subirlos a HDFS:

    hdfs dfs -mkdir -p /Tarea3
    hdfs dfs -put contratos.csv /Tarea3/

3. Instalar la librería de gráficos:

    pip install matplotlib

4. Ejecutar el análisis:

    spark-submit analisis_contratos.py

Los gráficos se guardan en la carpeta ~/resultados.


1. Carga el CSV desde HDFS.
2. Limpia los datos: convierte valores y fechas a su tipo correcto, elimina registros
   sin valor, sin departamento o sin fecha, y filtra años fuera de rango.
3. Calcula, por departamento, tipo de contrato, modalidad, estado y año: número de
   contratos, valor total, valor promedio y porcentaje pagado.
4. Genera gráficos de barras y de evolución anual, incluyendo un detalle de Nariño.


- `analisis_contratos.py`: análisis en batch con PySpark.
- `resultados/`: gráficos generados por el script.

Se usaron los primeros 300.000 registros que devuelve la API, no una muestra aleatoria.

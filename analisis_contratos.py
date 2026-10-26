import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RUTA = "hdfs://localhost:9000/Tarea3/contratos.csv"
SALIDA = os.path.expanduser("~/resultados")
os.makedirs(SALIDA, exist_ok=True)

spark = SparkSession.builder \
    .appName("Tarea3_Batch_SECOP") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ---------------- 1. CARGA ----------------
raw = spark.read.csv(RUTA, header=True, inferSchema=False)
total_inicial = raw.count()
print("\n=== 1. CARGA ===")
print("Filas cargadas:", total_inicial)
raw.printSchema()

# ---------------- 2. LIMPIEZA Y TRANSFORMACION ----------------
df = raw
for c in ["departamento", "ciudad", "estado_contrato",
          "tipo_de_contrato", "modalidad_de_contratacion"]:
    df = df.withColumn(c, F.trim(F.col(c)))

df = (df
      .withColumn("valor_del_contrato", F.col("valor_del_contrato").cast("double"))
      .withColumn("valor_pagado", F.col("valor_pagado").cast("double"))
      .withColumn("fecha_firma", F.to_date(F.substring("fecha_de_firma", 1, 10)))
      .withColumn("anio", F.year("fecha_firma")))

print("\n=== 2. VALORES NULOS (antes de limpiar) ===")
df.select([F.sum(F.col(c).isNull().cast("int")).alias(c)
           for c in ["departamento", "tipo_de_contrato", "modalidad_de_contratacion",
                     "valor_del_contrato", "fecha_firma"]]).show()

df = df.filter(F.col("valor_del_contrato").isNotNull()
               & (F.col("valor_del_contrato") > 0)
               & F.col("departamento").isNotNull()
               & F.col("tipo_de_contrato").isNotNull()
               & F.col("fecha_firma").isNotNull()
               & F.col("anio").between(2015, 2026))

df = df.withColumn("valor_pagado", F.coalesce(F.col("valor_pagado"), F.lit(0.0)))
df.cache()

total_limpio = df.count()
print("Filas despuÃ©s de limpiar:", total_limpio,
      "(se descartaron", total_inicial - total_limpio, ")")
print("Entidades distintas:", df.select("nombre_entidad").distinct().count())
print("Proveedores distintos:", df.select("proveedor_adjudicado").distinct().count())

# ---------------- 3. ANALISIS EXPLORATORIO ----------------
def resumen(columna):
    return (df.groupBy(columna)
              .agg(F.count("*").alias("contratos"),
                   F.round(F.sum("valor_del_contrato") / 1e9, 2).alias("valor_mil_millones"),
                   F.round(F.avg("valor_del_contrato") / 1e6, 2).alias("valor_prom_millones"),
                   F.round(F.sum("valor_pagado") / F.sum("valor_del_contrato") * 100, 1).alias("pct_pagado"))
              .orderBy(F.desc("contratos")))

def barras(datos, indice, titulo, etiqueta, archivo):
    etiquetas = [str(r[0]) for r in datos]
    valores = [r[indice] for r in datos]
    plt.figure(figsize=(11, 6))
    plt.barh(etiquetas[::-1], valores[::-1])
    plt.xlabel(etiqueta)
    plt.title(titulo)
    plt.tight_layout()
    plt.savefig(os.path.join(SALIDA, archivo))
    plt.close()

print("\n=== 3.1 TOP 10 DEPARTAMENTOS ===")
r_depto = resumen("departamento").limit(10)
r_depto.show(truncate=False)
filas = r_depto.collect()
barras(filas, 1, "Top 10 departamentos por nÃºmero de contratos", "Contratos", "01_contratos_por_departamento.png")
barras(filas, 2, "Valor contratado en esos departamentos", "Miles de millones de COP", "02_valor_por_departamento.png")

print("\n=== 3.2 POR TIPO DE CONTRATO ===")
r_tipo = resumen("tipo_de_contrato").limit(8)
r_tipo.show(truncate=False)
barras(r_tipo.collect(), 1, "Contratos por tipo de contrato", "Contratos", "03_contratos_por_tipo.png")

print("\n=== 3.3 POR MODALIDAD DE CONTRATACION ===")
r_mod = resumen("modalidad_de_contratacion").limit(10)
r_mod.show(truncate=False)
barras(r_mod.collect(), 1, "Contratos por modalidad de contrataciÃ³n", "Contratos", "04_contratos_por_modalidad.png")

print("\n=== 3.4 POR ESTADO DEL CONTRATO ===")
resumen("estado_contrato").show(truncate=False)

print("\n=== 3.5 EVOLUCION POR AÃ‘O ===")
r_anio = (df.groupBy("anio")
            .agg(F.count("*").alias("contratos"),
                 F.round(F.sum("valor_del_contrato") / 1e9, 2).alias("valor_mil_millones"))
            .orderBy("anio"))
r_anio.show()
filas = r_anio.collect()
plt.figure(figsize=(10, 5))
plt.plot([str(r[0]) for r in filas], [r[1] for r in filas], marker="o")
plt.title("Contratos firmados por aÃ±o")
plt.ylabel("Contratos")
plt.tight_layout()
plt.savefig(os.path.join(SALIDA, "05_contratos_por_anio.png"))
plt.close()

print("\n=== 3.6 NARIÃ‘O: CIUDADES CON MAS CONTRATOS ===")
r_nar = (df.filter(F.upper(F.col("departamento")) == "NARIÃ‘O")
           .groupBy("ciudad")
           .agg(F.count("*").alias("contratos"),
                F.round(F.sum("valor_del_contrato") / 1e9, 2).alias("valor_mil_millones"))
           .orderBy(F.desc("contratos")).limit(10))
r_nar.show(truncate=False)
barras(r_nar.collect(), 1, "NariÃ±o: ciudades con mÃ¡s contratos", "Contratos", "06_narino_ciudades.png")

print("\nGrÃ¡ficos guardados en:", SALIDA)
spark.stop()

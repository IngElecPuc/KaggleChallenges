# ingesta_ieee_fraud_to_pg.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import re

# ======== CONFIGURACIÓN ========
CSV_DIR = r"E:\Datasets\ieee-fraud-detection"  # <- tu carpeta
PG_URL  = "jdbc:postgresql://localhost:5432/Graphs"
PG_USER = "spark_ingest"
PG_PASS = "GYleZAI2pTBKJYl9W1PL"
PG_SCHEMA = "raw"

# Nombre de archivo -> nombre de tabla destino (sin esquema)
FILES = {
    "train_identity.csv":     "train_identity",
    "train_transaction.csv":  "train_transaction",
    "test_identity.csv":      "test_identity",
    "test_transaction.csv":   "test_transaction",
}

# Ajusta según tu CPU/IO. 4-8 suele ir bien en local para ~600MB por archivo.
NUM_PARTITIONS = 6
JDBC_BATCHSIZE = 20000

# ======== SPARK ========
spark = (
    SparkSession.builder
    .appName("ieee-fraud-to-postgres")
    # El driver JDBC se baja solo con --packages en el spark-submit
    .getOrCreate()
)

spark.conf.set("spark.sql.files.maxRecordsPerFile", 0)
spark.conf.set("spark.sql.shuffle.partitions", str(max(4, NUM_PARTITIONS)))
spark.conf.set("spark.sql.caseSensitive", "false")

def normalize_col(name: str) -> str:
    # minúsculas, sustituir espacios y caracteres raros por _
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9_]", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def load_csv(filename: str):
    path = f"{CSV_DIR}\\{filename}"
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")          # staging rápido; luego puedes tipificar en SQL
        .option("multiLine", "false")
        .option("escape", "\"")
        .option("quote", "\"")
        .option("nullValue", "")
        .option("mode", "PERMISSIVE")
        .option("maxCharsPerColumn", "1000000") # por si hay campos largos
        .csv(path)
    )
    # normalizar columnas
    new_cols = [normalize_col(c) for c in df.columns]
    for old, new in zip(df.columns, new_cols):
        if old != new:
            df = df.withColumnRenamed(old, new)
    # Reparticionar para IO balanceado
    df = df.repartition(NUM_PARTITIONS)
    return df

def write_pg(df, table_name: str):
    # Escribir a schema.table; modo overwrite para idempotencia inicial
    full_table = f"{PG_SCHEMA}.{table_name}"
    (
        df.write
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", full_table)
        .option("user", PG_USER)
        .option("password", PG_PASS)
        .option("driver", "org.postgresql.Driver")
        # Rendimiento y compatibilidad
        .option("stringtype", "unspecified")      
        .option("reWriteBatchedInserts", "true")  
        .option("batchsize", str(JDBC_BATCHSIZE))
        .mode("overwrite")
        .save()
    )

def main():
    for fname, tname in FILES.items():
        print(f"==> Cargando {fname} ...")
        df = load_csv(fname)
        print(f"   Columnas: {len(df.columns)} | Registros estimados: {df.count()}")
        print(f"==> Escribiendo en {PG_SCHEMA}.{tname} ...")
        write_pg(df, tname)
        print(f"   OK: {PG_SCHEMA}.{tname}")

if __name__ == "__main__":
    main()

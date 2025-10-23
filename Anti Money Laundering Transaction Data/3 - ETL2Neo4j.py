from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from py2neo import Graph
from datetime import timedelta
import sys, os
import multiprocessing
cpu_cores = multiprocessing.cpu_count()
print(f"CPU cores disponibles: {cpu_cores}")

# === Credenciales ===
PG_URL  = 'jdbc:postgresql://localhost:5432/graphs'
PG_USER = 'spark_ingest'
PG_PASS = 'GYleZAI2pTBKJYl9W1PL'
PG_SCHEMA = 'saml_d'
PG_TABLE1 = 'accounts'
PG_TABLE2 = 'transferences'
PG_TABLE3 = 'statements'

JDBC_JAR = r"C:\spark\spark-4.0.1-bin-hadoop3\jars\postgresql-42.7.4.jar"
JDBC_BATCHSIZE = 10000
JDBC_FETCHSIZE = 10000

NEO4J_JAR  = r"C:\spark\spark-4.0.1-bin-hadoop3\jars\neo4j-connector-apache-spark_2.13-5.3.11-SNAPSHOT_for_spark_3.jar"
NEO4J_URI  = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "Banco.69"
NEO4J_DDBB = "saml-d"

PYTHON = sys.executable  # python del kernel Jupyter

spark = (
    SparkSession.builder
    .appName("postgres-to-neo4j-graph")
    .master("local[*]")
    # === JARs locales ===
    .config("spark.jars", f"{JDBC_JAR},{NEO4J_JAR}")
    .config("spark.driver.extraClassPath", f"{JDBC_JAR};{NEO4J_JAR}")
    .config("spark.executor.extraClassPath", f"{JDBC_JAR};{NEO4J_JAR}")
    # === Mismo Python en driver/worker + fixes Windows ===
    .config("spark.pyspark.driver.python", PYTHON)
    .config("spark.pyspark.python", PYTHON)
    .config("spark.executorEnv.PYSPARK_PYTHON", PYTHON)
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.python.use.daemon", "false")
    .config("spark.local.dir", r"C:\spark\tmp")
    .config("spark.sql.shuffle.partitions", "128")
    .config("spark.driver.memory", "8g")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")  # Opcional: mejora performance
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

jdbc_props = {
    "user": PG_USER,
    "password": PG_PASS,
    "driver": "org.postgresql.Driver",
    "fetchsize": str(JDBC_FETCHSIZE)
}

# Accounts
accounts_df = (spark.read.format("jdbc")
    .option("url", PG_URL)
    .option("dbtable", f"{PG_SCHEMA}.{PG_TABLE1}")
    .option("partitionColumn", "account")
    .option("lowerBound", 1)                  
    .option("upperBound", 2000000)
    .option("numPartitions", 16)               
    .options(**jdbc_props)
    .load())
#Para particionado eficiente JDBC
acc_bounds = accounts_df.select(
    F.min("account").cast("long").alias("lo"),
    F.max("account").cast("long").alias("hi")
).first()
acc_lo, acc_hi = int(acc_bounds["lo"]), int(acc_bounds["hi"])

# Transferences
tx_df = (spark.read.format("jdbc")
    .option("url", PG_URL)
    .option("dbtable", f"{PG_SCHEMA}.{PG_TABLE2}")
    .option("partitionColumn", "id")
    .option("lowerBound", 1)
    .option("upperBound", 9500000)
    .option("numPartitions", 64)
    .options(**jdbc_props)
    .load())

# Statements
stm_df = (spark.read.format("jdbc")
    .option("url", PG_URL)
    .option("dbtable", f"{PG_SCHEMA}.{PG_TABLE3}")
    .option("partitionColumn", "account")       # particionamos por cuenta
    .option("lowerBound", acc_lo)
    .option("upperBound", acc_hi)
    .option("numPartitions", 64)                 # ajústalo a tu máquina/cluster
    .options(**jdbc_props)
    .load()
    .select(
        F.col("account").cast("long").alias("account"),
        F.col("date_time").alias("date_time"),
        F.col("txn_id").cast("long").alias("txn_id"),
        F.col("direction").alias("direction"),
        F.col("delta_amount").cast("double").alias("delta_amount"),
        F.col("running_balance").cast("double").alias("running_balance")
    )
)

accounts_df.printSchema()
tx_df.printSchema()
stm_df.printSchema()

# Nodos
nodes_df = accounts_df.select(
    F.col("account").cast("long").alias("account_number"),
    F.col("location").alias("location")
).dropDuplicates(["account_number"])

# Aristas
edges_df = tx_df.select(
    F.col("id").cast("long").alias("id"),
    F.col("date_time").alias("timestamp"),
    F.col("sender_account").cast("long").alias("src"),
    F.col("receiver_account").cast("long").alias("dst"),
    F.col("amount").cast("double").alias("amount"),
    F.col("payment_currency"),
    F.col("received_currency"),
    F.col("payment_type"),
    F.col("is_laundering").cast("int"),
    F.col("laundering_type")
)

# masked ~ Bernoulli(0.2)
edges_df = edges_df.withColumn("masked", (F.rand(seed=42) < F.lit(0.2)).cast("int"))

# Opcional: particiona por destino para paralelismo estable
edges_df = edges_df.repartition(256, "src")
nodes_df = nodes_df.repartition(64, "account_number")

# --- 1) STATEMENTS sin ventanas: seq y moneda lado a lado ---

# Secuencia cronológica de movimientos por cuenta (para ambos lados)
w_seq = Window.partitionBy("account").orderBy(F.col("date_time").asc(), F.col("txn_id").asc())
stm_seq = (stm_df
    .withColumn("seq", F.row_number().over(w_seq))
)

# Necesitamos moneda por movimiento según lado:
#   - si delta>0 (recibe) -> received_currency
#   - si delta<0 (envía)  -> payment_currency
tx_cur = (tx_df
    .select(
        F.col("id").alias("txn_id"),
        F.col("payment_currency"),
        F.col("received_currency")
    )
)

stm_cur = (stm_seq
    .join(tx_cur, on="txn_id", how="left")
    .withColumn("currency",
        F.when(F.col("delta_amount") > 0, F.col("received_currency"))
         .otherwise(F.col("payment_currency"))
    )
)

# --- 2) Ledger por lado: emisor (DEBIT) y receptor (CREDIT) ---

# Emisor: delta<0
sender_ledger = (stm_cur
    .filter(F.col("delta_amount") < 0)
    .select(
        F.col("txn_id").alias("id"),
        F.col("account").alias("src"),
        F.col("delta_amount").alias("src_delta"),
        F.col("running_balance").alias("src_balance_after"),
        (F.col("running_balance") - F.col("delta_amount")).alias("src_balance_before"),
        F.col("seq").alias("src_seq"),
        F.col("currency").alias("src_currency")
    )
)

# Receptor: delta>0
receiver_ledger = (stm_cur
    .filter(F.col("delta_amount") > 0)
    .select(
        F.col("txn_id").alias("id"),
        F.col("account").alias("dst"),
        F.col("delta_amount").alias("dst_delta"),
        F.col("running_balance").alias("dst_balance_after"),
        (F.col("running_balance") - F.col("delta_amount")).alias("dst_balance_before"),
        F.col("seq").alias("dst_seq"),
        F.col("currency").alias("dst_currency")
    )
)

# --- 3) Enriquecer aristas con ambos lados (sin ventanas) ---
edges_enriched = (edges_df
    .join(sender_ledger, on=["id", "src"], how="left")
    .join(receiver_ledger, on=["id", "dst"], how="left")
    .repartition(256, "src")
)

# --- 4) NODOS: estado macro (first/last/current) ---

# Último movimiento por cuenta
w_last = Window.partitionBy("account").orderBy(F.col("date_time").desc(), F.col("txn_id").desc())
last_rows = (stm_cur
    .withColumn("rn", F.row_number().over(w_last))
    .filter(F.col("rn")==1)
    .select(
        F.col("account"),
        F.col("running_balance").alias("current_balance"),
        F.col("date_time").alias("last_seen")
    )
)

first_rows = (stm_cur
    .groupBy("account")
    .agg(F.min("date_time").alias("first_seen"))
)

nodes_base = (nodes_df.alias("n")
    .join(last_rows.alias("lr"), F.col("n.account_number")==F.col("lr.account"), "left")
    .join(first_rows.alias("fr"), F.col("n.account_number")==F.col("fr.account"), "left")
    .drop("account")
    .repartition(64, "account_number")
)

# --- 5) Cierres mensuales por moneda ---

# Marca de año/mes
stm_monthly = (stm_cur
    .withColumn("year",  F.year("date_time"))
    .withColumn("month", F.month("date_time"))
)

# Último evento del mes por (account,currency,year,month)
w_month_last = Window.partitionBy("account","currency","year","month") \
                     .orderBy(F.col("date_time").desc(), F.col("txn_id").desc())

monthly_last = (stm_monthly
    .withColumn("rn", F.row_number().over(w_month_last))
    .filter(F.col("rn")==1)
    .select(
        "account","currency","year","month",
        F.col("running_balance").alias("balance_close")
    )
)

# Pivot a columnas tipo mclose_YYYY_MM_CUR
monthly_pivot = (monthly_last
    .withColumn("prop_name",
        F.concat(
            F.lit("mclose_"),
            F.format_string("%04d", F.col("year")),
            F.lit("_"),
            F.format_string("%02d", F.col("month")),
            F.lit("_"),
            F.col("currency")
        )
    )
    .select(
        F.col("account").alias("account_number"),
        "prop_name", "balance_close"
    )
)

# Compactar a un mapa (prop_name -> balance) y luego expandir al escribir
monthly_map = (monthly_pivot
    .groupBy("account_number")
    .agg(F.map_from_entries(F.collect_list(F.struct("prop_name","balance_close"))).alias("mclose_map"))
)

# Unir al nodo base
nodes_enriched_df = (nodes_base
    .join(monthly_map, on="account_number", how="left")
    # NOTA: neo4j connector no expande mapas a propiedades; podemos:
    #  (a) expandir en Spark a columnas (si el nº de props es acotado), o
    #  (b) escribir el mapa como JSON y expandirlo luego con APOC.
    # Aquí te dejo (a): expandimos a columnas reales.
)

# Expandir columnas desde el mapa (versión segura para 1 año / pocas monedas):
# 1) Listamos todas las claves existentes
all_keys = (monthly_pivot.select("prop_name").distinct().collect())
keys = [r["prop_name"] for r in all_keys]

# 2) Añadimos una columna por clave
for k in keys:
    nodes_enriched_df = nodes_enriched_df.withColumn(k, nodes_enriched_df["mclose_map"].getItem(F.lit(k)))

# 3) Limpiamos el mapa
nodes_enriched_df = nodes_enriched_df.drop("mclose_map")

graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS), name=NEO4J_DDBB)
graph.run("""
CREATE CONSTRAINT account_unique IF NOT EXISTS
FOR (a:Account) REQUIRE a.account_number IS UNIQUE
""")
graph.run("""
CREATE CONSTRAINT tx_unique IF NOT EXISTS
FOR ()-[r:TX]-() REQUIRE r.id IS UNIQUE
""")

# --- StayAwake: evita suspensión en Windows ---
import ctypes, platform, time

class StayAwake:
    """Bloquea suspensión/apagado de pantalla mientras el contexto está activo."""
    ES_CONTINUOUS = 0x80000000
    ES_SYSTEM_REQUIRED = 0x00000001
    ES_AWAYMODE_REQUIRED = 0x00000040  # opcional: evita que entre en sleep por "Away Mode"

    def __enter__(self):
        if platform.system() == "Windows":
            ctypes.windll.kernel32.SetThreadExecutionState(
                self.ES_CONTINUOUS | self.ES_SYSTEM_REQUIRED | self.ES_AWAYMODE_REQUIRED
            )
        return self

    def __exit__(self, exc_type, exc, tb):
        if platform.system() == "Windows":
            # Restablece al estado normal
            ctypes.windll.kernel32.SetThreadExecutionState(self.ES_CONTINUOUS)

import math, json, os, time
from datetime import timedelta

CHK_DIR = r"E:\Felpipe\Trabajo\Ciencias de datos en general\KaggleChallenges\Anti Money Laundering Transaction Data\checkpoints"   # directorio de checkpoints
os.makedirs(CHK_DIR, exist_ok=True)

def write_done(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    open(path, "w").close()

def is_done(path):
    return os.path.exists(path)

def count_df(df):
    # cuenta y devuelve int
    return df.count()

def estimate_eta(done, total, start_ts):
    now = time.time()
    elapsed = now - start_ts
    rate = done / elapsed if done > 0 else 0.0
    remaining = total - done
    eta_s = (remaining / rate) if rate > 0 else float("inf")
    pct = (done / total * 100.0) if total else 0.0
    return pct, timedelta(seconds=int(eta_s)), timedelta(seconds=int(elapsed))

def ingest_nodes(
    nodes_df,
    buckets=8,
    writers_per_bucket=2,
    batch_size=20000,
    chk_prefix="nodes_hashbuck"
):
    nodes_buck = (nodes_df
        .withColumn("bucket", (F.abs(F.hash("account_number")) % F.lit(buckets)))
        .repartition(buckets, "bucket")
        .persist())
    _ = nodes_buck.count()

    sizes = (nodes_buck.groupBy("bucket").count().collect())
    bucket_sizes = {int(r["bucket"]): int(r["count"]) for r in sizes}
    total = sum(bucket_sizes.values()); print(f"[NODOS] total={total}  buckets={buckets}")

    done, start = 0, time.time()
    for b in range(buckets):
        size_b = bucket_sizes.get(b, 0)
        tag = os.path.join(CHK_DIR, f"{chk_prefix}_{b}._DONE")
        if size_b == 0: write_done(tag); continue
        if is_done(tag):
            done += size_b; pct, eta, elapsed = estimate_eta(done, total, start)
            print(f"[NODOS] Skip bucket {b} ({size_b}). done={done}/{total} ({pct:0.2f}%) ETA={eta} elapsed={elapsed}")
            continue

        batch_df = nodes_buck.filter(F.col("bucket")==b).drop("bucket")

        t0 = time.time()
        (batch_df
            .coalesce(writers_per_bucket)
            .write
            .format("org.neo4j.spark.DataSource")
            .mode("Append")
            .option("url", "bolt://localhost:7687")
            .option("authentication.type","basic")
            .option("authentication.basic.username", NEO4J_USER)
            .option("authentication.basic.password", NEO4J_PASS)
            .option("database", NEO4J_DDBB)
            .option("labels", ":Account")
            .option("node.keys", "account_number")
            .option("batch.size", str(batch_size))
            .option("transaction.retries", "20")
            .option("transaction.retry.timeout", "60000")
            .save())
        t1 = time.time()

        done += size_b; write_done(tag)
        pct, eta, elapsed = estimate_eta(done, total, start)
        print(f"[NODOS] bucket {b} -> {size_b} filas en {timedelta(seconds=int(t1-t0))}. "
              f"done={done}/{total} ({pct:0.2f}%) ETA={eta} elapsed={elapsed})")
        
def ingest_edges(
    edges_df,
    buckets=16,
    writers_per_bucket=1,
    batch_size=20000,
    chk_prefix="rels_srcbuck"
):
    edges_buck = (edges_df
        .withColumn("bucket", (F.abs(F.hash("src")) % F.lit(buckets)))
        .repartition(buckets, "bucket")
        .sortWithinPartitions("src", "id")
        .persist())

    counts_by_bucket = (edges_buck.groupBy("bucket").count().collect())
    bucket_sizes = {int(r["bucket"]): int(r["count"]) for r in counts_by_bucket}
    total = sum(bucket_sizes.values()); print(f"[RELS] total={total}  buckets={buckets}")

    done, start = 0, time.time()
    for b in range(buckets):
        size_b = bucket_sizes.get(b, 0)
        tag = os.path.join(CHK_DIR, f"{chk_prefix}_{b}._DONE")
        if size_b == 0: write_done(tag); continue
        if is_done(tag):
            done += size_b; pct, eta, elapsed = estimate_eta(done, total, start)
            print(f"[RELS] Skip bucket {b} ({size_b}). done={done}/{total} ({pct:0.2f}%) ETA={eta} elapsed={elapsed}")
            continue

        batch_df = edges_buck.filter(F.col("bucket")==b).drop("bucket")

        t0 = time.time()
        (batch_df
            .coalesce(writers_per_bucket)
            .write
            .format("org.neo4j.spark.DataSource")
            .mode("Append")
            .option("url", "bolt://localhost:7687")
            .option("authentication.type", "basic")
            .option("authentication.basic.username", NEO4J_USER)
            .option("authentication.basic.password", NEO4J_PASS)
            .option("database", NEO4J_DDBB)
            .option("relationship", "TX")
            .option("relationship.save.strategy", "keys")
            .option("relationship.keys", "id")
            .option("relationship.source.labels", ":Account")
            .option("relationship.target.labels", ":Account")
            .option("relationship.source.node.keys", "src:account_number")
            .option("relationship.target.node.keys", "dst:account_number")
            .option("relationship.source.save.mode", "Match")
            .option("relationship.target.save.mode", "Match")
            .option("relationship.properties",
                    "timestamp,amount,payment_currency,received_currency,"
                    "payment_type,is_laundering,laundering_type,masked,"
                    "src_delta,src_balance_before,src_balance_after,src_seq,src_currency,"
                    "dst_delta,dst_balance_before,dst_balance_after,dst_seq,dst_currency")
            .option("batch.size", str(batch_size))
            .option("transaction.retries", "20")
            .option("transaction.retry.timeout", "60000")
            .save())
        t1 = time.time()

        done += size_b; write_done(tag)
        pct, eta, elapsed = estimate_eta(done, total, start)
        print(f"[RELS] bucket {b} -> {size_b} filas en {timedelta(seconds=int(t1-t0))}. "
              f"done={done}/{total} ({pct:0.2f}%) ETA={eta} elapsed={elapsed})")
        
inicio = time.time()
with StayAwake():
    # Nodos (micro-lotes)
    ingest_nodes(nodes_enriched_df, 
                      buckets=8, 
                      writers_per_bucket=2, 
                      batch_size=20000)

    # Relaciones (micro-lotes)
    ingest_edges(edges_enriched, 
                 buckets=16, 
                 writers_per_bucket=4, 
                 batch_size=20000, 
                 chk_prefix="rels_srcbuck" )

fin = time.time()
print(f"Tiempo total: {timedelta(seconds=int(fin - inicio))}")
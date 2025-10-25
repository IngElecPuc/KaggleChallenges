from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from py2neo import Graph
from datetime import timedelta
import yaml, sys, os, platform
import ctypes, multiprocessing
import time, subprocess, atexit, signal
from pathlib import Path


cpu_cores = multiprocessing.cpu_count()
print(f"CPU cores disponibles: {cpu_cores}")

with open("config.yaml", "r") as f:
    CFG = yaml.safe_load(f)

IS_WIN          = platform.system() == "Windows"
SPARK_LOCAL_DIR = CFG["spark"]["local_dirs"]["windows" if IS_WIN else "linux"]
PG_URL          = CFG["postgres"]["url"]
PG_USER         = CFG["postgres"]["user"]
PG_PASS         = CFG["postgres"]["pass"]
PG_SCHEMA       = CFG["postgres"]["schema_out"]["schema_name"]
PG_TABLE1       = CFG["postgres"]["schema_out"]["table1"]
PG_TABLE2       = CFG["postgres"]["schema_out"]["table2"]
PG_TABLE3       = CFG["postgres"]["schema_out"]["table3"]
JDBC_BATCHSIZE  = CFG["postgres"]["batchsize"]
JDBC_FETCHSIZE  = CFG["postgres"]["fetchsize"]
NEO4J_URI       = CFG["neo4j"]["uri"]
NEO4J_USER      = CFG["neo4j"]["user"]
NEO4J_PASS      = CFG["neo4j"]["pass"]
NEO4J_DDBB      = CFG["neo4j"]["database"]

PYTHON = sys.executable  # python del kernel Jupyter

def _in_notebook() -> bool:
    try:
        from IPython import get_ipython  # noqa
        return "IPKernelApp" in get_ipython().config
    except Exception:
        return False

def resolve_checkpoint_dir(cfg) -> Path:
    opt = cfg["etl"]["checkpoints"]
    enabled = bool(opt.get("enabled", True))
    if not enabled:
        return None

    mode = (opt.get("mode") or "auto").lower()
    subdir = opt.get("subdir") or ".etl_checkpoints"

    # 1) base_dir según modo
    if mode == "cwd":
        base = Path.cwd()
    elif mode == "script":
        # Puede fallar en notebook (no hay __file__)
        base = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
    elif mode == "custom":
        raw = opt.get("custom_dir") or ""
        base = Path(raw).expanduser().resolve() if raw else Path.cwd()
    else:  # "auto"
        if _in_notebook():
            base = Path.cwd()
        else:
            base = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()

    chk = (base / subdir).resolve()
    chk.mkdir(parents=True, exist_ok=True)
    return chk

CHK_DIR = resolve_checkpoint_dir(CFG)  # Path | None
print(f"Checkpoint dir: {CHK_DIR if CHK_DIR else 'DISABLED'}")

def _flag_path(prefix: str, bucket: int):
    if CHK_DIR is None:
        return None
    return (CHK_DIR / f"{prefix}_{bucket}._DONE").resolve()

def was_done(prefix: str, bucket: int) -> bool:
    p = _flag_path(prefix, bucket)
    return p.exists() if p else False

def mark_done(prefix: str, bucket: int) -> None:
    p = _flag_path(prefix, bucket)
    if p:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.touch()


builder = (SparkSession.builder
           .appName("postgres-to-neo4j-graph")
           .master("local[*]")
           .config("spark.pyspark.driver.python", PYTHON)
           .config("spark.pyspark.python", PYTHON)
           .config("spark.executorEnv.PYSPARK_PYTHON", PYTHON)
           .config("spark.driver.bindAddress", "127.0.0.1")
           .config("spark.driver.host", "127.0.0.1")
           .config("spark.python.use.daemon", "false")
           .config("spark.local.dir", SPARK_LOCAL_DIR)
           .config("spark.sql.shuffle.partitions", str(CFG["spark"]["shuffle_partitions"]))
           .config("spark.driver.memory", CFG["spark"]["driver_memory"])
           .config("spark.sql.execution.arrow.pyspark.enabled", "true")
           .config("spark.jars.packages", ",".join(CFG["spark"]["maven_packages"]))
          )
spark = builder.getOrCreate()
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

class StayAwake:
    """
    Mantiene el sistema despierto mientras el contexto está activo.
    - Windows: SetThreadExecutionState
    - Linux: systemd-inhibit con un proceso bloqueante
    """
    def __init__(self, reason: str = "Spark ETL running"):
        self.reason = reason
        self.proc = None
        self.is_win = platform.system() == "Windows"

    def __enter__(self):
        if self.is_win:
            # ES_CONTINUOUS(0x80000000) | ES_SYSTEM_REQUIRED(0x00000001) | ES_AWAYMODE_REQUIRED(0x00000040)
            self._win_set(True)
            atexit.register(self._win_set, False)
        else:
            # Lanza un proceso que se queda bloqueado hasta que salgamos
            # --what=sleep:idle evita suspensión e inactividad
            self.proc = subprocess.Popen(
                ["systemd-inhibit", "--what=sleep:idle", f"--why={self.reason}",
                 "--mode=block", "tail", "-f", "/dev/null"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, preexec_fn=os.setsid
            )
            atexit.register(self._linux_stop)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.is_win:
            self._win_set(False)
        else:
            self._linux_stop()

    def _win_set(self, enable: bool):
        ES_CONTINUOUS = 0x80000000
        ES_SYSTEM_REQUIRED = 0x00000001
        ES_AWAYMODE_REQUIRED = 0x00000040
        val = ES_CONTINUOUS
        if enable:
            val |= (ES_SYSTEM_REQUIRED | ES_AWAYMODE_REQUIRED)
        ctypes.windll.kernel32.SetThreadExecutionState(val)

    def _linux_stop(self):
        if self.proc and self.proc.poll() is None:
            try:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
            except Exception:
                pass

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
        if size_b == 0:
            mark_done(chk_prefix, b)
            continue
        if was_done(chk_prefix, b):
            done += size_b
            pct, eta, elapsed = estimate_eta(done, total, start)
            print(f"[NODOS] Skip bucket {b} ({size_b}). done={done}/{total} ({pct:0.2f}%) ETA={eta} elapsed={elapsed}")
            continue

        batch_df = nodes_buck.filter(F.col("bucket")==b).drop("bucket")

        t0 = time.time()
        (batch_df
            .coalesce(writers_per_bucket)
            .write
            .format("org.neo4j.spark.DataSource")
            .mode("Append")
            .option("url", NEO4J_URI)
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

        done += size_b
        mark_done(chk_prefix, b)
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
        if size_b == 0:
            mark_done(chk_prefix, b)
            continue
        if was_done(chk_prefix, b):
            done += size_b
            pct, eta, elapsed = estimate_eta(done, total, start)
            print(f"[RELS] Skip bucket {b} ({size_b}). done={done}/{total} ({pct:0.2f}%) ETA={eta} elapsed={elapsed}")
            continue

        batch_df = edges_buck.filter(F.col("bucket")==b).drop("bucket")

        t0 = time.time()
        (batch_df
            .coalesce(writers_per_bucket)
            .write
            .format("org.neo4j.spark.DataSource")
            .mode("Append")
            .option("url", NEO4J_URI)
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

        done += size_b
        mark_done(chk_prefix, b)
        pct, eta, elapsed = estimate_eta(done, total, start)
        print(f"[RELS] bucket {b} -> {size_b} filas en {timedelta(seconds=int(t1-t0))}. "
              f"done={done}/{total} ({pct:0.2f}%) ETA={eta} elapsed={elapsed})")
        
inicio = time.time()
with StayAwake():
    # Nodos (micro-lotes)
    ingest_nodes(nodes_enriched_df, 
                      buckets=CFG["etl"]["buckets"]["nodes"], 
                      writers_per_bucket=CFG["etl"]["writers_per_bucket"]["nodes"], 
                      batch_size=CFG["etl"]["batch_size"]["nodes"])

    # Relaciones (micro-lotes)
    ingest_edges(edges_enriched, 
                 buckets=CFG["etl"]["buckets"]["edges"], 
                 writers_per_bucket=CFG["etl"]["writers_per_bucket"]["edges"], 
                 batch_size=CFG["etl"]["batch_size"]["edges"], 
                 chk_prefix="rels_srcbuck" )

fin = time.time()
print(f"Tiempo total: {timedelta(seconds=int(fin - inicio))}")
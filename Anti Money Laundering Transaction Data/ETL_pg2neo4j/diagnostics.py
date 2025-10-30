from pyspark.sql import DataFrame
from pyspark.sql import functions as F, types as T
import os, shutil, math, multiprocessing, psutil
from contextlib import contextmanager
import psutil

FIXED_SIZES = {
    T.BooleanType: 1, T.ByteType: 1, T.ShortType: 2, T.IntegerType: 4,
    T.LongType: 8, T.FloatType: 4, T.DoubleType: 8,
    T.DateType: 4, T.TimestampType: 12,  # aprox
    T.BinaryType: 1,  # por byte; usamos avg length
}

def avg_strlen(df, col):
    return df.select(F.avg(F.length(F.col(col)).cast("double"))).first()[0] or 0.0

def approx_bytes_per_row_uncompressed(df, sample_rows=500_000):
    # muestreamos para las medias de strings y para evitar full scan
    sdf = df.limit(sample_rows)
    total = 0.0
    for f in df.schema.fields:
        dt = type(f.dataType)
        if isinstance(f.dataType, T.StringType):
            total += avg_strlen(sdf, f.name)
        elif isinstance(f.dataType, T.BinaryType):
            total += avg_strlen(sdf, f.name)  # bytes
        else:
            total += FIXED_SIZES.get(dt, 8)  # fallback
    # overhead por fila (objeto/serde) muy grosero:
    total += 16
    return total  # bytes por fila aprox

def estimate_for_ingest(df, target_bucket_mb=128, sample_rows=500_000):
    rows = df.count()
    bpr  = approx_bytes_per_row_uncompressed(df, sample_rows)
    total_bytes = bpr * rows
    target_bytes = target_bucket_mb * 1024 * 1024
    buckets = max(1, math.ceil(total_bytes / target_bytes))
    return {
        "rows": rows,
        "bytes_per_row_uncompressed": bpr,
        "total_gb": total_bytes / (1024**3),
        "target_bucket_mb": target_bucket_mb,
        "recommended_buckets": buckets,
    }

# Intentamos psutil para medir memoria; si no está, usamos /proc/meminfo
def _meminfo_fallback():
    info = {}
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                k, v = line.split(":")
                info[k.strip()] = int(v.strip().split()[0]) * 1024  # kB -> bytes
        total = info.get("MemTotal", 0)
        free  = info.get("MemAvailable", info.get("MemFree", 0))
        used  = max(0, total - free)
        return total, used, free
    except Exception:
        return 0, 0, 0

def get_machine_stats():
    cpu_cores = multiprocessing.cpu_count()
    try:
        vm = psutil.virtual_memory()
        total, used, avail = vm.total, vm.used, vm.available
    except Exception:
        total, used, avail = _meminfo_fallback()
    return {
        "cpu_cores": cpu_cores,
        "mem_total_bytes": int(total),
        "mem_used_bytes": int(used),
        "mem_available_bytes": int(avail),
    }

def human_bytes(n):
    for unit in ["B","KB","MB","GB","TB"]:
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}"
        n /= 1024.0

def print_diagnostics(logger, prefix="[DIAG] "):
    stats = get_machine_stats()
    logger.info(prefix + f"Cores totales          : {stats['cpu_cores']}")
    logger.info(prefix + f"RAM total              : {human_bytes(stats['mem_total_bytes'])}")
    logger.info(prefix + f"RAM usada              : {human_bytes(stats['mem_used_bytes'])}")
    logger.info(prefix + f"RAM disponible         : {human_bytes(stats['mem_available_bytes'])}")
    print(prefix + f"Cores totales          : {stats['cpu_cores']}")
    print(prefix + f"RAM total              : {human_bytes(stats['mem_total_bytes'])}")
    print(prefix + f"RAM usada              : {human_bytes(stats['mem_used_bytes'])}")
    print(prefix + f"RAM disponible         : {human_bytes(stats['mem_available_bytes'])}")
    return stats

def apply_spark_tuning(
    SparkSession,
    leave_one_core=True,
    cpus_per_task=2,
    target_post_shuffle_mb=128,
    min_shuffle_parts=16,
    speculation=False,
    g1gc=True
):
    """
    - Deja 1 core libre y limita concurrencia real via spark.task.cpus.
    - AQE con target de 128MB por partición post-shuffle (configurable).
    - Serializer Kryo + G1GC (opcional) para reducir overhead de heap.
    Devuelve (spark, params_dict).
    """
    stats = get_machine_stats()
    cpu_total = stats["cpu_cores"]
    usable = max(1, cpu_total - 1) if leave_one_core else cpu_total
    # Concurrencia efectiva ≈ floor(usable / cpus_per_task)
    eff_parallelism = max(1, usable // max(1, cpus_per_task))

    print("[TUNE] CPU totales  :", cpu_total)
    print("[TUNE] Cores a usar :", usable, "(dejando 1 libre)" if leave_one_core else "")
    print("[TUNE] cpus por task:", cpus_per_task, "=> concurrencia efectiva ~", eff_parallelism)

    builder = (SparkSession.builder
               .appName("ETL-postgres-neo4j")
               .master(f"local[{usable}]")
               # Serializador + GC amable
               .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
               .config("spark.sql.shuffle.partitions", str(max(eff_parallelism*2, min_shuffle_parts)))
               .config("spark.task.cpus", str(cpus_per_task))
               .config("spark.speculation", str(speculation).lower())
               # AQE
               .config("spark.sql.adaptive.enabled", "true")
               .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
               .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
               .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize",
                       str(int(target_post_shuffle_mb * 1024 * 1024))))
    if g1gc:
        builder = (builder
                   .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35")
                   .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35"))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    params = {
        "cpu_total": cpu_total,
        "usable_cores": usable,
        "cpus_per_task": cpus_per_task,
        "effective_parallelism": eff_parallelism,
        "target_post_shuffle_mb": target_post_shuffle_mb,
        "spark_sql_shuffle_partitions": int(spark.conf.get("spark.sql.shuffle.partitions")),
    }
    print("[TUNE] spark.sql.shuffle.partitions =", params["spark_sql_shuffle_partitions"])
    print("[TUNE] AQE target post-shuffle size =", target_post_shuffle_mb, "MB")
    return spark, params

@contextmanager
def tmpdir(path):
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)

def estimate_bytes_per_row(df, sample_frac=0.01, max_rows=2_000_000, tmp="/tmp/_spark_size_probe"):
    """
    Estima bytes/fila escribiendo una muestra a Parquet y midiendo el tamaño.
    - Limita la muestra a evitar desbordes.
    - Devuelve (bytes_per_row_est, rows_in_sample)
    """

    count_df = df.limit(max_rows)  # evita contar algo monstruoso si hay filtros previos
    approx_rows = count_df.count()  # recuento acotado
    frac = min(sample_frac, 1.0)
    if approx_rows <= 0:
        return 0.0, 0
    sample = df.sample(withReplacement=False, fraction=frac, seed=42)
    # garantizamos al menos unas filas
    sample = sample.limit(max(10_000, int(approx_rows * frac)))

    with tmpdir(tmp) as t:
        sample.coalesce(1).write.mode("overwrite").parquet(t)
        # busca el archivo parquet real
        fsize = 0
        for root, _, files in os.walk(t):
            for f in files:
                if f.endswith(".parquet"):
                    fsize += os.path.getsize(os.path.join(root, f))
        sample_rows = sample.count()
    if sample_rows == 0 or fsize == 0:
        return 0.0, sample_rows
    bpr = float(fsize) / float(sample_rows)
    return bpr, sample_rows

def recommend_partitions(
    df,
    label,
    spark_params,
    target_partition_mb=128,
    hard_max_parts=None,
    min_parts=None
):
    """
    Calcula particiones recomendadas según bytes/fila y target MB/partición.
    Usa df.count() para precisión (puede costar), imprime diagnóstico y devuelve num_parts.
    """
    print(f"[PART] Estimando particiones para {label}…")
    bpr, sample_rows = estimate_bytes_per_row(df, sample_frac=0.01)
    if bpr <= 0:
        print(f"[PART] No se pudo estimar bytes/row para {label}; usaré fallback por cores.")
        # Fallback: 2x la concurrencia efectiva
        num_parts = max(spark_params["effective_parallelism"] * 2, 16)
        return num_parts

    total_rows = df.count()
    target_bytes = target_partition_mb * 1024 * 1024
    rows_per_part = max(1, int(target_bytes / bpr))
    est_parts = max(1, int(math.ceil(total_rows / rows_per_part)))

    eff_par = spark_params["effective_parallelism"]
    # límites razonables
    if min_parts is None:
        min_parts = max(16, eff_par * 2)
    if hard_max_parts is None:
        hard_max_parts = max(64, eff_par * 8)

    num_parts = max(min_parts, min(est_parts, hard_max_parts))

    print(f"[PART] {label}:")
    print(f"       filas totales        ≈ {total_rows:,}")
    print(f"       bytes por fila (est) ≈ {bpr:.1f} B")
    print(f"       target MB/part       = {target_partition_mb} MB")
    print(f"       filas/part (objetivo)= {rows_per_part:,}")
    print(f"       particiones estimadas= {est_parts}")
    print(f"       particiones finales  = {num_parts}  (min={min_parts}, max={hard_max_parts})")
    return num_parts

def repartition_safely(df, num_parts, by_cols=None):
    """
    Reparticiona controlando el número final de particiones. Si by_cols se dan, usa hash-partition por esas columnas.
    """
    if by_cols:
        return df.repartition(int(num_parts), *[c for c in by_cols])
    return df.repartition(int(num_parts))

def recommend_writers_for_neo4j(spark_params, max_writers_per_bucket=2):
    """
    Sugiere writers por bucket para el conector Neo4j sin saturar la concurrencia efectiva.
    """
    eff = spark_params["effective_parallelism"]
    # deja siempre una holgura: writers_totales <= eff
    # si tienes buckets=B y writers_per_bucket=W, entonces B*W <= eff
    # sugerimos W en {1..max} y que B lo controles en el YAML.
    if eff <= 2:
        return 1
    return min(max_writers_per_bucket, max(1, eff // 2))

# si approx_bytes_per_row_uncompressed / estimate_for_ingest ya las tienes, reutilízalas:
# from ETL_pg2neo4j.diagnostics import estimate_for_ingest

def _decide_target_bucket_mb(free_gb: float, eff: int, kind: str) -> int:
    """
    Calcula un tamaño de bucket conservador a partir de la RAM libre (GB) y la concurrencia efectiva (eff).
    La idea: reservar ~2 GB para el driver y repartir el resto entre writers concurrentes,
    usando un factor de seguridad (0.5) y límites [64, 256] MB por bucket.
    """
    reserve_gb = 2.0
    usable_gb = max(0.5, free_gb - reserve_gb)
    writers = max(1, eff)
    per_writer_gb = (usable_gb / writers) * 0.5  # 50% de seguridad

    # relaciones suelen ser más pesadas -> un poco más alto
    base_mb = 128 if kind == "edges" else 96

    target_mb = int(max(64, min(256, per_writer_gb * 1024)))
    # mezcla con el base
    return int(max(64, min(256, (target_mb + base_mb) / 2)))

def get_effective_parallelism_from_spark(spark, leave_one_core=True):
    """
    Calcula la concurrencia efectiva a partir de la SparkSession existente,
    sin crear una nueva. Usa defaultParallelism y spark.task.cpus.
    """
    sc = spark.sparkContext
    total = max(1, sc.defaultParallelism)  # ~ núm. cores del cluster local
    if leave_one_core and total > 1:
        total -= 1
    task_cpus = int(spark.conf.get("spark.task.cpus", "1"))
    eff = max(1, total // max(1, task_cpus))
    return {
        "cpu_total_guess": sc.defaultParallelism,
        "usable_cores": total,
        "task_cpus": task_cpus,
        "effective_parallelism": eff,
    }

def ingestion_plan(spark, logger, df: DataFrame, *, kind: str, status: dict, BS: int = 10_000):
    """
    Devuelve (B, W, BS) sin crear otra SparkSession.
    """
    sparams = get_effective_parallelism_from_spark(spark, leave_one_core=True)
    eff = int(sparams["effective_parallelism"])

    # writers por bucket “sanos”
    W_default = 1 if eff <= 2 else 2

    # 2) tamaño de bucket objetivo según memoria libre
    free_bytes = status.get("mem_available_bytes") or int(status.get("mem_free_gb", 4) * 1024**3)
    free_gb = free_bytes / (1024**3)
    target_bucket_mb = _decide_target_bucket_mb(free_gb, eff, kind)

    # 3) estima dataset y buckets recomendados
    plan = estimate_for_ingest(df, target_bucket_mb=target_bucket_mb)
    B = max(2, int(plan["recommended_buckets"]))

    # 4) writers por bucket (no pasarse del paralelismo real)
    W = max(1, min(W_default, eff // max(1, B)))

    logger.info(
        f"[PLAN {kind}] rows={plan['rows']:,} "
        f"bpr≈{plan['bytes_per_row_uncompressed']:.1f}B "
        f"total≈{plan['total_gb']:.2f}GB "
        f"target_bucket≈{target_bucket_mb}MB -> buckets={B} writers/bucket={W} batch.size={BS} "
        f"(eff={eff}, task.cpus={sparams['task_cpus']}, usable_cores={sparams['usable_cores']})"
    )
    print(
        f"[PLAN {kind}] rows={plan['rows']:,} "
        f"bpr≈{plan['bytes_per_row_uncompressed']:.1f}B "
        f"total≈{plan['total_gb']:.2f}GB "
        f"target_bucket≈{target_bucket_mb}MB -> buckets={B} writers/bucket={W} batch.size={BS} "
        f"(eff={eff}, task.cpus={sparams['task_cpus']}, usable_cores={sparams['usable_cores']})"
    )
    return B, W, BS
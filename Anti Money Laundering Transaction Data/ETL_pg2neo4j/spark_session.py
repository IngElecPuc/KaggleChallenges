import os
from pyspark.sql import SparkSession
from pathlib import Path
from ETL_pg2neo4j.load_config import (
    CFG, SPARK_LOCAL_DIR, PYTHON, PG_USER, 
    PG_PASS, JDBC_FETCHSIZE, IS_WIN, LOG_DIR
)

def _normalize_jars(value):
    if isinstance(value, (list, tuple)):
        items = list(value)
    else:
        items = [p.strip() for p in str(value).split(",") if str(value).strip()]
    return [p for p in items if p and Path(p).exists()]

def _jprop(key: str, value: Path | str) -> str:
    v = str(value)
    if " " in v:
        v = f'"{v}"'
    return f"-D{key}={v}"

def get_spark(stats):
    if CFG['spark']['use_packages']:
        spark_jars_key = "spark.jars.packages"
        spark_jars_value = ",".join(CFG["spark"]["maven_packages"])
        set_jars = True
    else:
        spark_jars_key = "spark.jars"
        local_jars = CFG["spark"]["local_jars"]["windows" if IS_WIN else "linux"]
        jars_list = _normalize_jars(local_jars)
        set_jars = len(jars_list) > 0
        # OJO: spark.jars -> separado por comas
        spark_jars_value = ",".join(jars_list)

    conf_path = str((Path(__file__).resolve().parent / "config" / "log4j2.properties"))
    driver_opts = " ".join([
        _jprop("log4j2.configurationFile", f"file:{conf_path}"),
        _jprop("LOG_DIR", LOG_DIR),
    ])
    executor_opts = driver_opts

    usable_cores = max(1, stats['cpu_cores'] - 1)

    builder = (SparkSession.builder
        .appName("postgres-to-neo4j-graph")
        .master(f"local[{usable_cores}]")
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
        .config("spark.driver.extraJavaOptions", driver_opts)
        .config("spark.executor.extraJavaOptions", executor_opts)
    )

    if set_jars and spark_jars_value:
        # spark.jars necesita comas
        builder = builder.config(spark_jars_key, spark_jars_value)
        print("[SPARK] usando jars locales ->", spark_jars_value)

        # extraClassPath necesita separador de classpath del SO (':' en Linux)
        cp_value = os.pathsep.join(spark_jars_value.split(","))  # <- aquí el cambio
        builder = (builder
            .config("spark.driver.extraClassPath", cp_value)
            .config("spark.executor.extraClassPath", cp_value)
        )
        print("[SPARK] classpath reforzado (driver/executors) ->", cp_value)
    else:
        print("[SPARK] ADVERTENCIA: no se encontraron jars locales; dependerá de $SPARK_HOME/jars")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        spark._jvm.java.lang.Class.forName("org.postgresql.Driver")
        print("[CHECK] Postgres JDBC REALMENTE visible")
    except Exception as e:
        print("[CHECK] Postgres JDBC NO visible:", e)

    try:
        spark._jvm.java.lang.Class.forName("org.neo4j.spark.DataSource")
        print("[CHECK] Neo4j connector REALMENTE visible")
    except Exception as e:
        print("[CHECK] Neo4j connector NO visible:", e)

    jdbc_props = {
        "user": PG_USER,
        "password": PG_PASS,
        "driver": "org.postgresql.Driver",
        "fetchsize": str(JDBC_FETCHSIZE)
    }
    return spark, jdbc_props

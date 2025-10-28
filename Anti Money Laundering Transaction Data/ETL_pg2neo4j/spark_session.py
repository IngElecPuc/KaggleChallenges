from pyspark.sql import SparkSession
from ETL_pg2neo4j.load_config import (
    CFG, SPARK_LOCAL_DIR, PYTHON, PG_USER, PG_PASS, JDBC_FETCHSIZE, IS_WIN
)

def get_spark(stats): 

    if CFG['spark']['use_packages']:
        # Descargar desde Maven (requiere Internet)
        spark_kars_key = "spark.jars.packages"
        spark_kars_value = ",".join(CFG["spark"]["maven_packages"])
    else:
        # Usar rutas locales
        spark_kars_key = "spark.jars"
        spark_kars_value = CFG["spark"]["local_jars"]["windows" if IS_WIN else "linux"]
    
    usable_cores = max(1, stats['cpu_cores'] - 1) #Limitar el uso de workers a n-1
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
            .config(spark_kars_key, spark_kars_value)
            )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    jdbc_props = {
        "user": PG_USER,
        "password": PG_PASS,
        "driver": "org.postgresql.Driver",
        "fetchsize": str(JDBC_FETCHSIZE)
    }

    return spark, jdbc_props
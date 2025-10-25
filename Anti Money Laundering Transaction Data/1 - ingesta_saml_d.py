
import yaml, os, re
from pyspark.sql import SparkSession

with open("config.yaml", "r") as f:
    CFG = yaml.safe_load(f)

PG_URL          = CFG["postgres"]["url"]
PG_USER         = CFG["postgres"]["user"]
PG_PASS         = CFG["postgres"]["pass"]
PG_SCHEMA       = CFG["postgres"]["schema_raw"]["schema_name"]
CSV_DIR         = CFG["csv"]["base_dir"]
FILES           = CFG["csv"]["files"]
NUM_PARTITIONS  = CFG["csv"]["num_partitions"]
JDBC_BATCHSIZE  = CFG["postgres"]["batchsize"]
JDBC_JARS       = CFG["spark"].get("jars", "")
extra = {}
if JDBC_JARS:
    extra = {
      "spark.jars": JDBC_JARS,
      "spark.driver.extraClassPath": JDBC_JARS.replace(",", ":"),
      "spark.executor.extraClassPath": JDBC_JARS.replace(",", ":")
    }

spark = (
    SparkSession.builder
    .appName(CFG["spark"]["app_name"])
    .config("spark.sql.shuffle.partitions", str(CFG["spark"]["shuffle_partitions"]))
    .config("spark.driver.memory", CFG["spark"]["driver_memory"])
    .configMap(extra) if hasattr(SparkSession.Builder, 'configMap') else SparkSession.builder
)

def normalize_col(name: str) -> str:
    # minúsculas, sustituir espacios y caracteres raros por _
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9_]", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def load_csv(filename: str):
    path = os.path.join(CSV_DIR, filename)
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
        .option("truncate", "true") 
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

    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import URL

    connection_url = URL.create(
        drivername='postgresql+psycopg2',
        username=PG_USER,
        password=PG_PASS,  
        host='localhost',
        port=5432,
        database='graphs',
        query={'sslmode': 'require'},
    )
    engine = create_engine(connection_url)

    stmts = [
        "ALTER TABLE raw.saml_d ADD COLUMN IF NOT EXISTS id BIGINT",
        "DO $$ BEGIN CREATE SEQUENCE raw.saml_d_id_seq; EXCEPTION WHEN duplicate_table THEN NULL; END $$;",
        "ALTER TABLE raw.saml_d ALTER COLUMN id SET DEFAULT nextval('raw.saml_d_id_seq')",
        "UPDATE raw.saml_d SET id = nextval('raw.saml_d_id_seq') WHERE id IS NULL",
        "ALTER TABLE raw.saml_d ALTER COLUMN id SET NOT NULL",
        "DO $$ BEGIN ALTER TABLE raw.saml_d ADD CONSTRAINT saml_d_pkey PRIMARY KEY (id); "
        "EXCEPTION WHEN duplicate_object THEN NULL; END $$;",
    ] #Agregar un id para poder particionar más adelante con pyspark al leer

    with engine.begin() as conn:  # transacción
        for s in stmts:
            conn.execute(text(s))

if __name__ == "__main__":
    main()
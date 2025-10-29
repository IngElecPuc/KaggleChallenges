from __future__ import annotations
import os, sys, json, glob, logging, platform
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine
from pyspark.sql import functions as F
from ETL_pg2neo4j.load_config import (
    PG_URL, PG_USER, PG_PASS, PG_SCHEMA_LOG, 
    PG_TABLE_LOG, LOG_DIR 
)
from datetime import datetime
from typing import Iterable, Dict, Any, List

APP_LOG = LOG_DIR / "app-python.log"     # logs de tu app Python
SPARK_LOG_GLOB = str(LOG_DIR / "spark-app*.log*")  # logs de Spark/JVM (JSON)

# ---------- DDL Postgres ----------
def instantiate_pglogs():
    pg_url = PG_URL.split(':')
    IP_DIR = pg_url[2].replace('//', '')
    port, ddbb = pg_url[-1].split('/')

    connection_url = URL.create(
        drivername='postgresql+psycopg2',
        username=PG_USER,
        password=PG_PASS,  
        host=IP_DIR,
        port=int(port),
        database=ddbb,
        query={'sslmode': 'disable'},
    )
    engine = create_engine(connection_url)

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA_LOG};"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {PG_SCHEMA_LOG}.{PG_TABLE_LOG} (
              id      bigserial PRIMARY KEY,
              ts      timestamptz,
              level   text,
              logger  text,
              thread  text,
              node    text,
              app     text,
              message text,
              kv      jsonb
            );
        """))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {PG_TABLE_LOG}_ts_idx ON {PG_SCHEMA_LOG}.{PG_TABLE_LOG} (ts);"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {PG_TABLE_LOG}_level_idx ON {PG_SCHEMA_LOG}.{PG_TABLE_LOG} (level);"))

# ---------- Ingesta logs de Spark (JVM) -> Postgres ----------
def write_spark_logs_to_pg(spark, app_name="postgres-to-neo4j-graph", node_label="driver"):
    # Ajusta el schema a tu JsonLayout. Ejemplos comunes:
    # timestamp (string ISO) ó timeMillis (long), level, loggerName/logger, thread, message, ...
    df = spark.read.json(SPARK_LOG_GLOB)

    # Normaliza campos: usa 'timestamp' si existe, si no timeMillis
    ts = F.coalesce(
        F.to_timestamp("timestamp"),
        F.to_timestamp(F.from_unixtime(F.col("timeMillis")/1000.0))
    ).alias("ts")

    level  = F.coalesce(F.col("level"), F.col("levelStr")).alias("level")
    logger = F.coalesce(F.col("logger"), F.col("loggerName")).alias("logger")
    thread = F.coalesce(F.col("thread"), F.col("threadName")).alias("thread")
    msg    = F.coalesce(F.col("message"), F.col("formattedMessage")).alias("message")

    logs_df2 = df.select(
        ts, level, logger, thread,
        F.lit(node_label).alias("node"),
        F.lit(app_name).alias("app"),
        msg,
        F.to_json(F.struct(*[c for c in df.columns])).cast("string").alias("kv")
    ).where(F.col("ts").isNotNull())

    (
      logs_df2.write
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", f"{PG_SCHEMA_LOG}.{PG_TABLE_LOG}")
        .option("user", PG_USER)
        .option("password", PG_PASS)
        .mode("append")
        .save()
    )

# ---------- Ingesta logs de tu app Python -> Postgres ----------
def _mk_engine_from_cfg() -> Engine:
    pg_url = PG_URL.split(':')
    host = pg_url[2].replace('//', '')
    port, ddbb = pg_url[-1].split('/')

    connection_url = URL.create(
        drivername='postgresql+psycopg2',
        username=PG_USER,
        password=PG_PASS,
        host=host,
        port=int(port),
        database=ddbb,
        query={'sslmode': 'disable'},
    )
    return create_engine(connection_url)

def _iter_python_log_records(log_glob: str) -> Iterable[Dict[str, Any]]:
    """
    Lee archivos JSONL (uno por línea) generados por tu JsonFormatter.
    Devuelve diccionarios listos para bindear en INSERT.
    """
    for path in glob.glob(log_glob):
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    # línea corrupta: la insertamos como texto crudo en 'message'
                    yield {
                        "ts": None,
                        "level": "ERROR",
                        "logger": "etl.parser",
                        "thread": None,
                        "node": platform.node(),
                        "app": "postgres-to-neo4j-graph",
                        "message": line[:8000],  # truncado defensivo
                        "kv": json.dumps({"raw": line})
                    }
                    continue

                # ts viene como "%Y-%m-%dT%H:%M:%S%z" según tu JsonFormatter
                ts_str = rec.get("ts")
                ts_val = None
                if ts_str:
                    try:
                        ts_val = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S%z")
                    except Exception:
                        # Si por algún motivo cambia el formato, lo dejamos en NULL
                        ts_val = None

                yield {
                    "ts": ts_val,  # SQLAlchemy lo enviará como timestamptz
                    "level": rec.get("level"),
                    "logger": rec.get("logger"),
                    "thread": rec.get("thread"),  # normalmente no lo pones; quedará NULL
                    "node": platform.node(),
                    "app": "postgres-to-neo4j-graph",
                    "message": rec.get("message"),
                    # guardamos todo el registro como jsonb adicional
                    "kv": json.dumps(rec, ensure_ascii=False),
                }

def write_python_logs_to_pg(
    engine: Engine | None = None,
    log_dir: Path | None = None,
    app_log_basename: str = "app-python.log",
    batch_size: int = 1000
) -> int:
    """
    Carga los logs de tu app Python (JsonFormatter) a Postgres usando SQLAlchemy.
    - engine: si no lo pasas, se crea desde la config.
    - log_dir: carpeta donde están los logs (por defecto LOG_DIR).
    - app_log_basename: nombre base del archivo (se hace glob con '*').
    - batch_size: tamaño de lote para inserción masiva.
    Devuelve la cantidad de filas insertadas.
    """
    if engine is None:
        engine = _mk_engine_from_cfg()

    # Resuelve rutas portables
    if log_dir is None:
        # Usa el mismo helper que uses en tu módulo (ej.: resolve_log_dir())
        from pathlib import Path
        log_dir = Path.home() / ".etl" / "logs"

    log_glob = str((Path(log_dir) / app_log_basename)) + "*"

    # Statement parametrizado (jsonb via CAST)
    insert_stmt = text(f"""
        INSERT INTO {PG_SCHEMA_LOG}.{PG_TABLE_LOG}
            (ts, level, logger, thread, node, app, message, kv)
        VALUES
            (:ts, :level, :logger, :thread, :node, :app, :message, CAST(:kv AS jsonb))
    """)

    total = 0
    batch: List[Dict[str, Any]] = []

    with engine.begin() as conn:
        for rec in _iter_python_log_records(log_glob):
            batch.append(rec)
            if len(batch) >= batch_size:
                conn.execute(insert_stmt, batch)
                total += len(batch)
                batch.clear()

        if batch:
            conn.execute(insert_stmt, batch)
            total += len(batch)

    return total

# ---------- Logger Python portable ----------
def get_python_logger():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    handler = TimedRotatingFileHandler(
        str(APP_LOG), when="D", interval=1, backupCount=30, encoding="utf-8"
    )

    class JsonFormatter(logging.Formatter):
        def format(self, record):
            return json.dumps({
                "ts": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S%z"),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
                "module": record.module,
                "funcName": record.funcName,
                "lineno": record.lineno
            }, ensure_ascii=False)

    logger = logging.getLogger("etl")
    logger.setLevel(logging.INFO)
    handler.setFormatter(JsonFormatter())
    # evita agregarse dos veces si llamas varias veces
    if not any(isinstance(h, TimedRotatingFileHandler) for h in logger.handlers):
        logger.addHandler(handler)
    return logger

# ---------- (Opcional) Redirigir stdout/err de TU código a logger ----------
class StreamToLogger:
    def __init__(self, log, level): self.log, self.level = log, level
    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.log.log(self.level, line)
    def flush(self): pass

# ---------- Ingesta logs de tu app Python -> Postgres ----------
# def write_python_logs_to_pg(app_name="postgres-to-neo4j-graph"):
#     import psycopg2
#     from psycopg2.extras import Json

#     pg_url = PG_URL.split(':')
#     host = pg_url[2].replace('//', '')
#     port, ddbb = pg_url[-1].split('/')

#     conn = psycopg2.connect(dbname=ddbb, user=PG_USER, password=PG_PASS, host=host, port=int(port))
#     cur = conn.cursor()

#     for path in glob.glob(str(APP_LOG) + "*"):
#         with open(path, "r", encoding="utf-8") as f:
#             for line in f:
#                 rec = json.loads(line)
#                 cur.execute(f"""
#                     INSERT INTO {PG_SCHEMA_LOG}.{PG_TABLE_LOG}
#                            (ts, level, logger, thread, node, app, message, kv)
#                     VALUES (to_timestamp(%s, 'YYYY-MM-DD"T"HH24:MI:SSOF'),
#                             %s, %s, NULL, %s, %s, %s, %s)
#                 """, (
#                     rec.get("ts"),
#                     rec.get("level"),
#                     rec.get("logger"),
#                     platform.node(),      # hostname
#                     app_name,
#                     rec.get("message"),
#                     Json(rec)             # jsonb
#                 ))
#     conn.commit()
#     cur.close(); conn.close()
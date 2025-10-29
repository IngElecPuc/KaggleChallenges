from pyspark.sql import functions as F
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from ETL_pg2neo4j.load_config import PG_URL, PG_USER, PG_PASS, PG_SCHEMA_LOG, PG_TABLE_LOG

logs_df = (
  spark.read
       .schema("timestamp string, level string, logger string, message string, thread string")
       .json("/var/log/etl/spark-app*.log*")  # incluye gz
)

logs_df2 = logs_df.select(
    F.to_timestamp("timestamp").alias("ts"),
    "level","logger","thread",
    F.lit("driver").alias("node"),
    F.lit("postgres-to-neo4j-graph").alias("app"),
    "message",
    F.lit(None).cast("string").alias("kv")   # si tu JsonLayout incluye más campos, empaquétalos aquí
)

(
  logs_df2.write
     .format("jdbc")
     .option("url", "jdbc:postgresql://HOST:5432/DB")
     .option("dbtable", "etl.logs")
     .option("user", "USER")
     .option("password", "PASS")
     .option("truncate", "false")
     .mode("append")
     .save()
)

import json, glob, psycopg2
conn = psycopg2.connect("dbname=DB user=USER password=PASS host=HOST port=5432")
cur = conn.cursor()

for path in glob.glob("/var/log/etl/app-python.log*"):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            cur.execute("""
                INSERT INTO etl.logs (ts, level, logger, message, app, kv)
                VALUES (to_timestamp(%s, 'YYYY-MM-DD\"T\"HH24:MI:SSOF'),
                        %s, %s, %s, %s, %s::jsonb)
            """, (rec["ts"], rec["level"], rec["logger"], rec["message"],
                  "postgres-to-neo4j-graph", json.dumps(rec)))
conn.commit()
cur.close(); conn.close()


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
    conn.execute(text(
        f"ALTER TABLE {PG_SCHEMA_OUT}.{PG_TABLE_OUT1} "
        f"ADD CONSTRAINT {PG_SCHEMA_OUT}_{PG_TABLE_OUT1}_pkey PRIMARY KEY (account)"
    ))
    conn.execute(text(
        f"ALTER TABLE {PG_SCHEMA_OUT}.{PG_TABLE_OUT2} "
        f"ADD CONSTRAINT {PG_SCHEMA_OUT}_{PG_TABLE_OUT2}_pkey PRIMARY KEY (id)"
    ))
    conn.execute(text(
        f"ALTER TABLE {PG_SCHEMA_OUT}.{PG_TABLE_OUT3} "
        f"ADD CONSTRAINT {PG_SCHEMA_OUT}_{PG_TABLE_OUT3}_pkey "
        f"PRIMARY KEY (account, date_time, txn_id)"
    ))

"""
CREATE SCHEMA IF NOT EXISTS etl;

CREATE TABLE IF NOT EXISTS etl.logs (
  id           bigserial PRIMARY KEY,
  ts           timestamptz,
  level        text,
  logger       text,
  thread       text,
  node         text,
  app          text,
  message      text,
  kv           jsonb
);

-- Índices útiles
CREATE INDEX IF NOT EXISTS logs_ts_idx ON etl.logs (ts);
CREATE INDEX IF NOT EXISTS logs_level_idx ON etl.logs (level);
"""

import logging, json, os, sys
from logging.handlers import TimedRotatingFileHandler

os.makedirs("/var/log/etl", exist_ok=True)
py_handler = TimedRotatingFileHandler(
    "/var/log/etl/app-python.log", when="D", interval=1, backupCount=30, encoding="utf-8"
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
py_handler.setFormatter(JsonFormatter())
logger.addHandler(py_handler)

# (Opcional) Redirige stdout/stderr de Spark local a tu logger
class StreamToLogger:
    def __init__(self, log, level): self.log, self.level = log, level
    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.log.log(self.level, line)
    def flush(self): pass

sys.stdout = StreamToLogger(logger, logging.INFO)
sys.stderr = StreamToLogger(logger, logging.ERROR)

# Uso:
logger.info("ETL iniciado")

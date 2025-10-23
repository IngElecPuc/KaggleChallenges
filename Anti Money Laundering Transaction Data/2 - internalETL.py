from pyspark.sql import SparkSession
import pyspark.pandas as ps
import os, sys
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
from pyspark.sql import functions as F, Window

PG_URL  = 'jdbc:postgresql://localhost:5432/graphs'
PG_USER = 'spark_ingest'
PG_PASS = 'GYleZAI2pTBKJYl9W1PL'
PG_SCHEMA_IN = 'raw'
PG_SCHEMA_OUT = 'saml_d'
PG_TABLE_IN =  'saml_d'
PG_TABLE_OUT1 =  'accounts'
PG_TABLE_OUT2 =  'transferences'
PG_TABLE_OUT3 =  'statements'
JDBC_JAR = r"C:\spark\spark-4.0.1-bin-hadoop3\jars\postgresql-42.7.4.jar"  
JDBC_BATCHSIZE = 10000
JDBC_FETCHSIZE = 10000

spark = (
    SparkSession.builder
    .appName("ieee-fraud-jupyter")
    .config("spark.jars", JDBC_JAR)
    .config("spark.driver.extraClassPath", JDBC_JAR)
    .config("spark.executor.extraClassPath", JDBC_JAR)
    .config("spark.sql.ansi.enabled", "false") #Para poder usar la API de pandas pues no soporta modo ansi
    .config("spark.pyspark.driver.python", sys.executable)
    .config("spark.pyspark.python", sys.executable)
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

df = (
    spark.read.format("jdbc")
    .option("url", PG_URL)
    .option("dbtable", f"{PG_SCHEMA_IN}.{PG_TABLE_IN}")  
    .option("user", PG_USER)
    .option("password", PG_PASS)
    .option("driver", "org.postgresql.Driver")
    .option("partitionColumn", "id")
    .option("lowerBound", "1")
    .option("upperBound", "10000000")
    .option("numPartitions", "6")
    .option("fetchsize", str(JDBC_FETCHSIZE))
    .load()
)

df.show(5)
df.printSchema()

pairs = (
    df.select(F.col("sender_account").alias("account"),
              F.col("sender_bank_location").alias("location"))
      .unionByName(
          df.select(F.col("receiver_account").alias("account"),
                    F.col("receiver_bank_location").alias("location"))
      )
      .filter(F.col("account").isNotNull() & F.col("location").isNotNull())
)

counts = pairs.groupBy("account", "location").count()

w = Window.partitionBy("account").orderBy(F.col("count").desc(), F.col("location").asc())
accounts = (
    counts.withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .select("account", "location")
)

(accounts.write
    .format("jdbc")
    .option("url", PG_URL)
    .option("dbtable", f"{PG_SCHEMA_OUT}.{PG_TABLE_OUT1}")
    .option("user", PG_USER)
    .option("password", PG_PASS)
    .option("driver", "org.postgresql.Driver")
    .option("batchsize", str(JDBC_BATCHSIZE))
    .option("truncate", "true") 
    .mode("overwrite")  # o 'append'
    .save())

transfers = df.withColumn(
    "datetime",
    F.to_timestamp(
        F.concat_ws(" ", F.col("date"), F.date_format(F.col("time"), "HH:mm:ss")),
        "yyyy-MM-dd HH:mm:ss"
    )
)

# Selecciona solo columnas que vas a escribir y fuerza tipos JVM simples
# (ajusta el listado a tus columnas verdaderas)
cols_out = [
    F.col("id").cast("long").alias("id"),
    F.col("datetime").cast("timestamp").alias("date_time"),
    F.col("sender_account").cast("long").alias("sender_account"),
    F.col("receiver_account").cast("long").alias("receiver_account"),
    F.col("amount").cast("double").alias("amount"),
    F.col("payment_currency").cast("string").alias("payment_currency"),
    F.col("received_currency").cast("string").alias("received_currency"),
    F.col("payment_type").cast("string").alias("payment_type"),
    F.col("is_laundering").cast("integer").alias("is_laundering"),
    F.col("laundering_type").cast("string").alias("laundering_type")
]
transfers = transfers.select(*cols_out)

# Materializa en JVM (evita recomputar nada de pandas)
transfers = transfers.persist()
_ = transfers.count()

(transfers.write
    .format("jdbc")
    .option("url", PG_URL)
    .option("dbtable", f"{PG_SCHEMA_OUT}.{PG_TABLE_OUT2}")
    .option("user", PG_USER)
    .option("password", PG_PASS)
    .option("driver", "org.postgresql.Driver")
    .option("stringtype", "unspecified")
    .option("batchsize", str(JDBC_BATCHSIZE))
    #.option("truncate", "true")
    .mode("overwrite")
    .save())

tx = transfers.filter(F.col("date_time").isNotNull())

#Entradas de dinero
credits = (
    tx.select(
        F.col("id").alias("txn_id"),
        F.col("date_time"),
        F.col("receiver_account").alias("account"),
        F.lit("CREDIT").alias("direction"),
        F.col("amount").cast("double").alias("amount_signed")
    )
)
#Salidas de dinero
debits = (
    tx.select(
        F.col("id").alias("txn_id"),
        F.col("date_time"),
        F.col("sender_account").alias("account"),
        F.lit("DEBIT").alias("direction"),
        (-F.col("amount")).cast("double").alias("amount_signed")
    )
)

movements = credits.unionByName(debits)
movements = movements.repartition(200, "account")

w_acc = (Window
         .partitionBy("account")
         .orderBy(F.col("date_time").asc(), F.col("txn_id").asc())
         .rowsBetween(Window.unboundedPreceding, Window.currentRow))

statements_full = (
    movements
    .withColumn("running_balance", F.sum("amount_signed").over(w_acc))
    .select(
        "account",
        "date_time",
        "txn_id",
        "direction",
        F.col("amount_signed").alias("delta_amount"),
        "running_balance"
    )
    .persist()
)

_ = statements_full.count()  # materializa

(statements_full.write
    .format("jdbc")
    .option("url", PG_URL)
    .option("dbtable", f"{PG_SCHEMA_OUT}.{PG_TABLE_OUT3}")  # p.ej. saml_d.statements
    .option("user", PG_USER)
    .option("password", PG_PASS)
    .option("driver", "org.postgresql.Driver")
    .option("batchsize", str(JDBC_BATCHSIZE))
    .mode("overwrite")
    .save())

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

connection_url = URL.create(
    drivername='postgresql+psycopg2',
    username=PG_USER,
    password=PG_PASS,  
    host='localhost',
    port=5432,
    database='graphs',
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
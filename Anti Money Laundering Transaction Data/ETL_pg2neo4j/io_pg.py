from pyspark.sql import functions as F
from ETL_pg2neo4j.load_config import PG_URL, PG_SCHEMA, PG_TABLE1, PG_TABLE2, PG_TABLE3


def read_accounts(spark, jdbc_props):
    return (spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", f"{PG_SCHEMA}.{PG_TABLE1}")
        .option("partitionColumn", "account")
        .option("lowerBound", 1)                  
        .option("upperBound", 2000000)
        .option("numPartitions", 16)               
        .options(**jdbc_props)
        .load())

def read_transferences(spark, jdbc_props):
    return (spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", f"{PG_SCHEMA}.{PG_TABLE2}")
        .option("partitionColumn", "id")
        .option("lowerBound", 1)
        .option("upperBound", 9500000)
        .option("numPartitions", 64)
        .options(**jdbc_props)
        .load())

def read_statements(spark, jdbc_props, accounts_df):

    #Para particionado eficiente JDBC
    acc_bounds = accounts_df.select(
        F.min("account").cast("long").alias("lo"),
        F.max("account").cast("long").alias("hi")
    ).first()
    acc_lo, acc_hi = int(acc_bounds["lo"]), int(acc_bounds["hi"])

    return (spark.read.format("jdbc")
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


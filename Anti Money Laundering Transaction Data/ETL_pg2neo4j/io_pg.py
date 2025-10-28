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
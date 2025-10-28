from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

def build_nodes_edges(accounts_df, tx_df, stm_df):
    
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
        #NOTE más adelante (spark 4+ la opción de arriba va a estar deprecada, cambiar por algo como esto)
        #for k in keys:  # k es un str de Python
        #nodes_enriched_df = nodes_enriched_df.withColumn(
        #    k, F.element_at("mclose_map", F.lit(k))
        #)
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

    return nodes_enriched_df, edges_enriched
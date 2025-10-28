
from datetime import timedelta
import time
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
from py2neo import Graph
from ETL_pg2neo4j.load_config import NEO4J_URI, NEO4J_USER, NEO4J_PASS, NEO4J_DDBB
from ETL_pg2neo4j.utils import estimate_eta, was_done, mark_done

def wait_for_db_online(sys_graph, db_name, timeout=10):
    """Espera hasta que la base esté online (máx. timeout segundos)"""
    for _ in range(timeout):
        status = sys_graph.run(
            "SHOW DATABASES YIELD name, currentStatus "
            "WHERE name=$db RETURN currentStatus",
            db=db_name
        ).evaluate()
        if status == "online":
            return True
        time.sleep(1)
    raise TimeoutError(f"La base {db_name} no llegó a estado ONLINE en {timeout}s")

def prepare_dataset_neo4j():

    graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS), name='system')

    print('Preparing Graph Database')

    graph.run(f'STOP DATABASE `{NEO4J_DDBB}`;')

    graph.run(f'DROP DATABASE `{NEO4J_DDBB}` IF EXISTS;')

    graph.run(f'CREATE DATABASE `{NEO4J_DDBB}` IF NOT EXISTS;')

    wait_for_db_online(graph, NEO4J_DDBB)
    graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS), name=NEO4J_DDBB)

    graph.run("""
    CREATE CONSTRAINT account_unique IF NOT EXISTS
    FOR (a:Account) REQUIRE a.account_number IS UNIQUE
    """)

    graph.run("""
    CREATE CONSTRAINT tx_unique IF NOT EXISTS
    FOR ()-[r:TX]-() REQUIRE r.id IS UNIQUE
    """)

    graph.run('SHOW DATABASES;')

def ingest_nodes(
    nodes_df,
    buckets=8,
    writers_per_bucket=2,
    batch_size=20000,
    chk_prefix="nodes_hashbuck"
):
    nodes_buck = (nodes_df
        .withColumn("bucket", (F.abs(F.hash("account_number")) % F.lit(buckets)))
        .repartition(200)
        .persist(StorageLevel.MEMORY_AND_DISK_DESER))
    _ = nodes_buck.count()

    sizes = (nodes_buck.groupBy("bucket").count().collect())
    bucket_sizes = {int(r["bucket"]): int(r["count"]) for r in sizes}
    total = sum(bucket_sizes.values()); print(f"[NODOS] total={total}  buckets={buckets}")

    done, start = 0, time.time()
    for b in range(buckets):
        size_b = bucket_sizes.get(b, 0)
        if size_b == 0:
            mark_done(chk_prefix, b)
            continue
        if was_done(chk_prefix, b):
            done += size_b
            pct, eta, elapsed = estimate_eta(done, total, start)
            print(f"[NODOS] Skip bucket {b} ({size_b}). done={done}/{total} ({pct:0.2f}%) ETA={eta} elapsed={elapsed}")
            continue

        batch_df = nodes_buck.filter(F.col("bucket")==b).drop("bucket")

        t0 = time.time()
        (batch_df
            .coalesce(writers_per_bucket)
            .write
            .format("org.neo4j.spark.DataSource")
            .mode("Append")
            .option("url", NEO4J_URI)
            .option("authentication.type","basic")
            .option("authentication.basic.username", NEO4J_USER)
            .option("authentication.basic.password", NEO4J_PASS)
            .option("database", NEO4J_DDBB)
            .option("labels", ":Account")
            .option("node.save.mode", "Merge")
            .option("node.keys", "account_number")
            .option("batch.size", str(batch_size))
            .option("transaction.retries", "3")
            .option("transaction.retry.timeout", "30000")
            .save())
        t1 = time.time()

        done += size_b
        mark_done(chk_prefix, b)
        pct, eta, elapsed = estimate_eta(done, total, start)
        print(f"[NODOS] bucket {b} -> {size_b} filas en {timedelta(seconds=int(t1-t0))}. "
              f"done={done}/{total} ({pct:0.2f}%) ETA={eta} elapsed={elapsed})")
        
    nodes_buck.unpersist()

def ingest_edges(
    edges_df,
    buckets=16,
    writers_per_bucket=1,
    batch_size=20000,
    chk_prefix="rels_srcbuck"
):
    edges_buck = (edges_df
        .withColumn("bucket", (F.abs(F.hash("src")) % F.lit(buckets)))
        .repartition(buckets, "bucket")
        .sortWithinPartitions("src", "id")
        .persist(StorageLevel.MEMORY_AND_DISK))

    counts_by_bucket = (edges_buck.groupBy("bucket").count().collect())
    bucket_sizes = {int(r["bucket"]): int(r["count"]) for r in counts_by_bucket}
    total = sum(bucket_sizes.values()); print(f"[RELS] total={total}  buckets={buckets}")

    done, start = 0, time.time()
    for b in range(buckets):
        size_b = bucket_sizes.get(b, 0)
        if size_b == 0:
            mark_done(chk_prefix, b)
            continue
        if was_done(chk_prefix, b):
            done += size_b
            pct, eta, elapsed = estimate_eta(done, total, start)
            print(f"[RELS] Skip bucket {b} ({size_b}). done={done}/{total} ({pct:0.2f}%) ETA={eta} elapsed={elapsed}")
            continue

        batch_df = edges_buck.filter(F.col("bucket")==b).drop("bucket")

        t0 = time.time()
        (batch_df
            .coalesce(writers_per_bucket)
            .write
            .format("org.neo4j.spark.DataSource")
            .mode("Append")
            .option("url", NEO4J_URI)
            .option("authentication.type", "basic")
            .option("authentication.basic.username", NEO4J_USER)
            .option("authentication.basic.password", NEO4J_PASS)
            .option("database", NEO4J_DDBB)
            .option("relationship", "TX")
            .option("relationship.save.strategy", "keys")
            .option("relationship.keys", "id")
            .option("relationship.source.labels", ":Account")
            .option("relationship.target.labels", ":Account")
            .option("relationship.source.node.keys", "src:account_number")
            .option("relationship.target.node.keys", "dst:account_number")
            .option("relationship.source.save.mode", "Match")
            .option("relationship.target.save.mode", "Match")
            .option("relationship.properties",
                    "timestamp,amount,payment_currency,received_currency,"
                    "payment_type,is_laundering,laundering_type,masked,"
                    "src_delta,src_balance_before,src_balance_after,src_seq,src_currency,"
                    "dst_delta,dst_balance_before,dst_balance_after,dst_seq,dst_currency")
            .option("batch.size", str(batch_size))
            .option("transaction.retries", "3")
            .option("transaction.retry.timeout", "30000")
            .save())
        t1 = time.time()

        done += size_b
        mark_done(chk_prefix, b)
        pct, eta, elapsed = estimate_eta(done, total, start)
        print(f"[RELS] bucket {b} -> {size_b} filas en {timedelta(seconds=int(t1-t0))}. "
              f"done={done}/{total} ({pct:0.2f}%) ETA={eta} elapsed={elapsed})")
        
    edges_buck.unpersist()
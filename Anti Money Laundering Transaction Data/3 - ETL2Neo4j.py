import time
from datetime import timedelta
from ETL_pg2neo4j.load_config import CFG
from ETL_pg2neo4j.diagnostics import print_diagnostics
from ETL_pg2neo4j.spark_session import get_spark
from ETL_pg2neo4j.io_pg import read_accounts, read_transferences, read_statements
from ETL_pg2neo4j.transform import build_nodes_edges
from ETL_pg2neo4j.io_neo4j import prepare_dataset_neo4j, ingest_nodes, ingest_edges
from ETL_pg2neo4j.utils import StayAwake

def main():

    #Diagnóstico inicial del sistema y del estado de éste
    stats = print_diagnostics()
    #Creación de la sesión de spark
    spark, jdbc_props = get_spark(stats)
    #Lectura de las cuentas, transacciones y saldos
    accounts_df = read_accounts(spark, jdbc_props)
    tx_df = read_transferences(spark, jdbc_props)
    stm_df = read_statements(spark, jdbc_props, accounts_df)
    #Despliegue
    accounts_df.printSchema()
    tx_df.printSchema()
    stm_df.printSchema()
    #Construcción del modelo de grafos
    nodes_enriched_df, edges_enriched = build_nodes_edges(accounts_df, tx_df, stm_df)
    #Preparación del raw de la base de datos en el motor
    prepare_dataset_neo4j()
    #Ingesta del modelo de grafos
    inicio = time.time()
    with StayAwake():
        # Nodos (micro-lotes)
        ingest_nodes(nodes_enriched_df, 
                        buckets=CFG["etl"]["buckets"]["nodes"], 
                        writers_per_bucket=CFG["etl"]["writers_per_bucket"]["nodes"], 
                        batch_size=CFG["etl"]["batch_size"]["nodes"])

        # Relaciones (micro-lotes)
        ingest_edges(edges_enriched, 
                    buckets=CFG["etl"]["buckets"]["edges"], 
                    writers_per_bucket=CFG["etl"]["writers_per_bucket"]["edges"], 
                    batch_size=CFG["etl"]["batch_size"]["edges"], 
                    chk_prefix="rels_srcbuck" )

    fin = time.time()
    print(f"Tiempo total: {timedelta(seconds=int(fin - inicio))}")

if __name__ == '__main__':
    main()
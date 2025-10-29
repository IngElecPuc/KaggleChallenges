import time, sys, logging, traceback
from datetime import timedelta
from ETL_pg2neo4j.load_config import CFG
from ETL_pg2neo4j.diagnostics import print_diagnostics
from ETL_pg2neo4j.spark_session import get_spark
from ETL_pg2neo4j.io_pg import read_accounts, read_transferences, read_statements
from ETL_pg2neo4j.transform import build_nodes_edges
from ETL_pg2neo4j.io_neo4j import prepare_dataset_neo4j, ingest_nodes, ingest_edges
from ETL_pg2neo4j.utils import StayAwake
from ETL_pg2neo4j.logs import (
    StreamToLogger, get_python_logger,
    instantiate_pglogs, write_python_logs_to_pg, write_spark_logs_to_pg
)

def main():
    logger = get_python_logger()
    sys.stdout = StreamToLogger(logger, logging.INFO)
    sys.stderr = StreamToLogger(logger, logging.ERROR)
    
    inicio = time.time()
    spark = None
    
    try:
        logger.info("ETL iniciado")
        print("ETL iniciado")
        #Diagnóstico inicial del sistema y del estado de éste
        stats = print_diagnostics(logger)

        #Asegura esquema/tabla de logs
        instantiate_pglogs()
        
        # Spark (ya con Log4j2 apuntando a LOG_DIR)
        spark, jdbc_props = get_spark(stats)

        # Lectura
        accounts_df = read_accounts(spark, jdbc_props)
        tx_df = read_transferences(spark, jdbc_props)
        stm_df = read_statements(spark, jdbc_props, accounts_df)

        
        accounts_df.printSchema()
        tx_df.printSchema()
        stm_df.printSchema()

        # Modelo
        nodes_enriched_df, edges_enriched = build_nodes_edges(accounts_df, tx_df, stm_df)
        logger.info("Modelo preparado en memoria")
        print("Modelo preparado en memoria")
        stats = print_diagnostics(logger)

        # Neo4j
        prepare_dataset_neo4j()
        with StayAwake():
            ingest_nodes(nodes_enriched_df, 
                buckets=CFG["etl"]["buckets"]["nodes"],
                writers_per_bucket=CFG["etl"]["writers_per_bucket"]["nodes"],
                batch_size=CFG["etl"]["batch_size"]["nodes"])
            ingest_edges(edges_enriched, 
                buckets=CFG["etl"]["buckets"]["edges"],
                writers_per_bucket=CFG["etl"]["writers_per_bucket"]["edges"],
                batch_size=CFG["etl"]["batch_size"]["edges"],
                chk_prefix="rels_srcbuck")

        logger.info("ETL finalizado OK")
        print("ETL finalizado OK")

    except Exception as e:
        print("ETL FAILED - Check logs")
        logger.error(f"ETL FAILED: {e}")
        logger.error(traceback.format_exc())
        raise
    finally:
        # Envía SIEMPRE lo que haya en archivos a Postgres (aunque haya fallado)
        try:
            if spark is not None:
                write_spark_logs_to_pg(spark)
        except Exception as ex:
            logger.error(f"ship spark logs failed: {ex}")
        try:
            rows = write_python_logs_to_pg()
            logger.info(f"python logs shipped: {rows}")
        except Exception as ex:
            # último recurso: imprime a stderr
            print(f"[WARN] ship python logs failed: {ex}", file=sys.stderr)

        if spark is not None:
            spark.stop()

        fin = time.time()
        logger.info(f"Tiempo total: {timedelta(seconds=int(fin - inicio))}")
        print(f"Tiempo total: {timedelta(seconds=int(fin - inicio))}")

if __name__ == '__main__':
    main()
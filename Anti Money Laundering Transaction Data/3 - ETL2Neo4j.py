import time, sys, logging, traceback
from datetime import timedelta
from ETL_pg2neo4j.load_config import CFG
from ETL_pg2neo4j.diagnostics import print_diagnostics, ingestion_plan
from ETL_pg2neo4j.spark_session import get_spark
from ETL_pg2neo4j.io_pg import read_accounts, read_transferences, read_statements
from ETL_pg2neo4j.transform import build_nodes_edges
from ETL_pg2neo4j.io_neo4j import prepare_dataset_neo4j, ingest_nodes, ingest_edges
from ETL_pg2neo4j.utils import StayAwake
from ETL_pg2neo4j.logs import (
    StreamToLogger, get_python_logger,
    instantiate_pglogs, write_python_logs_to_pg, write_spark_logs_to_pg
)

import os, requests
from contextlib import ContextDecorator

class TelegramNotify(ContextDecorator):
    def __init__(self, ok_msg="Calculo finalizado", err_prefix="Calculo falló"):
        self.ok_msg = ok_msg
        self.err_prefix = err_prefix
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.url = f"https://api.telegram.org/bot{self.token}/sendMessage"

    def _send(self, text):
        try:
            requests.post(self.url, json={"chat_id": self.chat_id, "text": text}, timeout=10).raise_for_status()
        except Exception as e:
            print(f"[telegram] fallo al notificar: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc is None:
            self._send(self.ok_msg)
            return False
        else:
            msg = f"{self.err_prefix}: {exc}"
            self._send(msg)
            # no suprime la excepción (deja que Jupyter la muestre)
            return False

def _notify(text):
    t, c = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if not t or not c: return
    try:
        requests.post(f"https://api.telegram.org/bot{t}/sendMessage",
                      json={"chat_id": c, "text": text}, timeout=8)
    except Exception:
        pass

def _exhook(exc_type, exc, tb):
    try: _notify(f"Calculo falló: {exc}")
    finally: sys.__excepthook__(exc_type, exc, tb)

sys.excepthook = _exhook


def main():
    logger = get_python_logger()
    logger.propagate = False
    if CFG.get("logging", {}).get("capture_stdout", True):
        sys.stdout = StreamToLogger(logger, logging.INFO)
        #sys.stderr = StreamToLogger(logger, logging.ERROR)
    
    start = time.time()
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
        nodes_enriched_df, edges_enriched_df = build_nodes_edges(accounts_df, tx_df, stm_df)
        last_step = start
        step = time.time()
        msg = f"Modelo preparado en memoria. Tiempo de cómputo {timedelta(seconds=int(step - last_step))}\n"
        logger.info(msg)
        print(msg)
        stats = print_diagnostics(logger)

        # Preparar plan de ingesta
        if CFG.get("spark", {}).get("calculate_plan", True):
            B_nodes, W_nodes, BS_nodes = ingestion_plan(spark, logger, nodes_enriched_df, 
                                                    kind="nodes", status=stats, BS=10_000)
            B_edges, W_edges, BS_edges = ingestion_plan(spark, logger, edges_enriched_df, 
                                                    kind="edges", status=stats, BS=10_000)
        else:
            etl_params  = CFG.get("etl", {})
            B_nodes     = etl_params.get('buckets', {}).get('nodes', int)
            W_nodes     = etl_params.get('writers_per_bucket', {}).get('nodes', int)
            BS_nodes    = etl_params.get('batch_size', {}).get('nodes', int)
            B_edges     = etl_params.get('buckets', {}).get('edges', int)
            W_edges     = etl_params.get('writers_per_bucket', {}).get('edges', int)
            BS_edges    = etl_params.get('batch_size', {}).get('edges', int)

        last_step = step
        step = time.time()
        msg = f"Plan de ingesta preparado. Tiempo de cómputo {timedelta(seconds=int(step - last_step))}\n"
        logger.info(msg)
        print(msg)

        # Neo4j
        prepare_dataset_neo4j()
        with StayAwake():

            ingest_nodes(nodes_enriched_df, buckets=B_nodes, writers_per_bucket=W_nodes,
                 batch_size=BS_nodes, chk_prefix="nodes_hashbuck")
            
            ingest_edges(edges_enriched_df, buckets=B_edges, writers_per_bucket=W_edges,
                 batch_size=BS_edges, chk_prefix="rels_srcbuck")

        last_step = step
        step = time.time()
        msg = f"Tiempo total de ingesta {timedelta(seconds=int(step - last_step))}\n"
        logger.info(msg)
        print(msg)
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

        stop = time.time()
        logger.info(f"Tiempo total: {timedelta(seconds=int(stop - start))}")
        print(f"Tiempo total: {timedelta(seconds=int(stop - start))}")

if __name__ == '__main__':
    #with TelegramNotify():
    #    main()

    try:
        main()
        _notify("Calculo finalizado")   # éxito
    except KeyboardInterrupt:
        _notify("Calculo interrumpido por el usuario")
        raise
    except Exception as e:
        # ya lo capturó sys.excepthook, pero por si acaso:
        _notify(f"Calculo falló: {e}")
        raise
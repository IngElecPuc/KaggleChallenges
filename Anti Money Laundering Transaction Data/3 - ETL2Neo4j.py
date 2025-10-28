"""
etl_pg_neo4j/
├─ etl_pg_neo4j/                    # paquete Python
│  ├─ __init__.py
│  ├─ app.py                        # punto de entrada (main) mínimo
│  ├─ config.py                     # carga/validación de config.yaml
│  ├─ log.py                        # logging unificado
│  ├─ spark_env/
│  │   ├─ __init__.py
│  │   ├─ diagnostics.py            # cores/RAM, prints de diagnóstico
│  │   ├─ tuning.py                 # creación de SparkSession y tuning AQE/GC
│  │   └─ partitioning.py           # estimación bytes/row y recomendación de particiones
│  ├─ io_pg/
│  │   ├─ __init__.py
│  │   └─ jdbc_read.py              # funciones de lectura JDBC (accounts/tx/stm)
│  ├─ transform/
│  │   ├─ __init__.py
│  │   ├─ nodes.py                  # construcción/enriquecimiento de nodos
│  │   └─ edges.py                  # construcción/enriquecimiento de aristas
│  ├─ io_neo4j/
│  │   ├─ __init__.py
│  │   ├─ writer.py                 # escrituras a Neo4j (nodos/aristas, buckets/writers)
│  │   └─ schema.py                 # constraints/indexes y checks en Neo4j
│  └─ utils/
│      ├─ __init__.py
│      ├─ df_checks.py              # sanity checks (nulls, conteos, duplicados)
│      └─ timeit.py                 # helpers de medición de etapas
├─ config.yaml                      # tu config actual
├─ README.md
├─ pyproject.toml / setup.cfg       # instalación editable (opcional)
└─ scripts/
   └─ submit_etl.sh                 # spark-submit con paquetes y confs
IDEA ORIGINAL CHATGPT
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
from py2neo import Graph
from datetime import timedelta
import yaml, sys, os, platform
import ctypes, multiprocessing
import time, subprocess, atexit, signal
from pathlib import Path


def main():
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
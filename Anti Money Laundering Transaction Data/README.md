# ETL Pipeline con Spark, PostgreSQL y Neo4j (Ubuntu + Windows)

Este proyecto implementa un pipeline de 3 etapas usando **Apache Spark 4.0.1**, **PostgreSQL** y **Neo4j Enterprise** para construir un grafo de transacciones a partir del dataset **SAML-D (Anti Money Laundering)**.

- **1 - ingesta_saml_d.py**  
  Ingesta el CSV en PostgreSQL (`raw.saml_d`), normaliza nombres de columnas, crea `id` secuencial y PK.

- **2 - internalETL.py**  
  Realiza un ETL dentro de PostgreSQL para generar tablas limpias:  
  `saml_d.accounts`, `saml_d.transferences`, `saml_d.statements`.

- **3 - ETL2Neo4j.py**  
  Exporta esas tablas a Neo4j mediante Spark Connector (nodos y relaciones), crea constraints si no existen.

> Los scripts soportan **Windows y Ubuntu**, seleccionan rutas automáticamente y usan `--packages` (sin necesidad de JAR locales).

> Dataset original: **Kaggle – Synthetic Transaction Monitoring Dataset (SAML-D)**  
> https://www.kaggle.com/datasets/berkanoztas/synthetic-transaction-monitoring-dataset-aml

---

## 1) Montaje de la carpeta de datos (Ubuntu)

Queremos acceder al dataset desde:

```
/mnt/datasets/Anti Money Laundering Transaction Data (SAML-D)
```

Si está bajo `/mnt/datasets/Datasets/...`, creamos un **bind mount**:

```bash
sudo mkdir -p "/mnt/datasets/Anti Money Laundering Transaction Data (SAML-D)"

sudo mount --bind \
  "/mnt/datasets/Datasets/Anti Money Laundering Transaction Data (SAML-D)" \
  "/mnt/datasets/Anti Money Laundering Transaction Data (SAML-D)"
```

(Opcional persistente en `/etc/fstab`):
```bash
/mnt/datasets/Datasets/Anti\040Money\040Laundering\040Transaction\040Data\040(SAML-D) \
/mnt/datasets/Anti\040Money\040Laundering\040Transaction\040Data\040(SAML-D)  none  bind  0  0
```

---

## 2) Configuración de PostgreSQL

Ejecutar estos comandos en `psql` como usuario `postgres` para crear la base y usuario del pipeline:

```sql
CREATE USER spark_ingest WITH PASSWORD 'GYleZAI2pTBKJYl9W1PL'; --O el password que tú quieras

-- Ver bases existentes
SELECT datname 
FROM pg_database
WHERE datistemplate = false;

-- Crear base si no existe
CREATE DATABASE graphs OWNER spark_ingest;

GRANT CONNECT ON DATABASE graphs TO spark_ingest;
GRANT CREATE  ON DATABASE graphs TO spark_ingest;

-- Dar acceso al esquema public
GRANT USAGE ON SCHEMA public TO spark_ingest;
GRANT CREATE ON SCHEMA public TO spark_ingest;

GRANT SELECT, INSERT, UPDATE, DELETE, REFERENCES, TRIGGER
ON ALL TABLES IN SCHEMA public TO spark_ingest;

GRANT USAGE, SELECT, UPDATE 
ON ALL SEQUENCES IN SCHEMA public TO spark_ingest;

GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO spark_ingest;

-- Que las tablas/secuencias futuras hereden permisos
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT, INSERT, UPDATE, DELETE, REFERENCES, TRIGGER ON TABLES TO spark_ingest;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO spark_ingest;

ALTER ROLE spark_ingest SET search_path = raw, public;
```

---

## 3) Neo4j Enterprise – crear o borrar base `saml-d`

```bash
# Crear base
cypher-shell -u neo4j -p 'Banco.69' -d system \
"CREATE DATABASE \`saml-d\` IF NOT EXISTS; SHOW DATABASES WHERE name = 'saml-d';"

# Borrar base
cypher-shell -u neo4j -p 'Banco.69' -d system \
"STOP DATABASE \`saml-d\`; DROP DATABASE \`saml-d\` IF EXISTS;"
```

---

## 4) Ejecución con Spark (Ubuntu)

Los scripts ya incluyen ajustes de red (`spark.driver.host`, `bindAddress`) y rutas por OS.  
Se usan `--packages` para cargar controladores JDBC y Neo4j.

```bash
# (1) CSV -> PostgreSQL (raw.saml_d)
/opt/spark/bin/spark-submit \
  --packages org.postgresql:postgresql:42.7.4 \
  "1 - ingesta_saml_d.py"

# (2) ETL intermedio -> accounts, transferences, statements
/opt/spark/bin/spark-submit \
  --packages org.postgresql:postgresql:42.7.4 \
  "2 - internalETL.py"

# (3) PostgreSQL -> Neo4j (nodos + relaciones)
/opt/spark/bin/spark-submit \
  --packages org.postgresql:postgresql:42.7.4,org.neo4j:neo4j-connector-apache-spark_2.13:5.3.10_for_spark_3 \
  "3 - ETL2Neo4j.py"

/opt/spark/bin/spark-submit \
  --packages org.postgresql:postgresql:42.7.4 \
  --conf spark.executor.memory=5g \
  --conf spark.driver.memory=5g \
  --conf spark.memory.fraction=0.7 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.shuffle.partitions=64 \
  --conf spark.task.maxFailures=1 \
  --conf spark.stage.maxConsecutiveAttempts=1 \
  --conf spark.speculation=false \
  "3 - ETL2Neo4j.py"

/opt/spark-3.5.6/bin/spark-submit \
  --packages org.postgresql:postgresql:42.7.4,org.neo4j:neo4j-connector-apache-spark_2.13:5.3.x_for_spark_4 \
  --conf spark.executor.memory=5g \
  --conf spark.driver.memory=5g \
  --conf spark.memory.fraction=0.7 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.shuffle.partitions=64 \
  --conf spark.task.maxFailures=1 \
  --conf spark.stage.maxConsecutiveAttempts=1 \
  --conf spark.speculation=false \

/opt/spark/bin/spark-submit \
  --packages org.postgresql:postgresql:42.7.4,org.neo4j:neo4j-connector-apache-spark_2.13:5.3.10_for_spark_3 \
  --conf spark.executor.memory=5g \
  --conf spark.driver.memory=5g \
  --conf spark.memory.fraction=0.7 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.shuffle.partitions=64 \
  --conf spark.task.maxFailures=1 \
  --conf spark.stage.maxConsecutiveAttempts=1 \
  --conf spark.speculation=false \
  "3 - ETL2Neo4j.py"

/opt/spark/bin/spark-submit \
  --packages org.postgresql:postgresql:42.7.4,org.neo4j:neo4j-connector-apache-spark_2.13:5.3.x_for_spark_4 \
  --conf spark.executor.memory=5g \
  --conf spark.driver.memory=5g \
  --conf spark.memory.fraction=0.7 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.shuffle.partitions=64 \
  --conf spark.task.maxFailures=1 \
  --conf spark.stage.maxConsecutiveAttempts=1 \
  --conf spark.speculation=false \
  "3 - ETL2Neo4j.py"

```

---

## 5) Helpers para iniciar/detener servicios en Ubuntu (opcional)

```bash
# Iniciar
sudo systemctl start postgresql || true
sudo systemctl start postgresql@14-main 2>/dev/null || true
sudo systemctl start postgresql@17-main 2>/dev/null || true
sudo systemctl start neo4j || true

# Detener
sudo systemctl stop neo4j || true
sudo systemctl stop postgresql || true
sudo systemctl stop postgresql@14-main 2>/dev/null || true
sudo systemctl stop postgresql@17-main 2>/dev/null || true
sudo pkill -f neo4j || true
sudo pkill -f postgres || true
```

---

## 6) Notas finales

- Editar rutas en `config.yaml` si cambias de equipo o disco.  
- `config.yaml` ya reduce particiones (`spark.shuffle.partitions`) y usa `MEMORY_AND_DISK` para evitar errores de memoria.  
- La interfaz web de Neo4j (`http://localhost:7474/browser`) no afecta la conexión Spark → Neo4j (que usa exclusivamente `bolt://localhost:7687`).  
- Para revisar el dataset, licencias o esquema original → visita Kaggle.

---

## 7) ⚠️ Checkpoints del ETL (control de reintentos por bucket)

El script **`3 - ETL2Neo4j.py`** implementa un sistema de checkpoints propio (no usa Spark Streaming), que permite evitar reprocesar buckets ya exportados a Neo4j.  
Cuando los checkpoints están activados, cada bucket procesado crea un archivo `. _DONE` en un directorio llamado `.etl_checkpoints`.  
Si el script se ejecuta nuevamente y encuentra estos archivos, mostrará mensajes como:

```bash
[NODOS] Skip bucket 0 (...)
[NODOS] Skip bucket 1 (...)
...
```

y no volverá a cargar esos datos.

### 📁 Ubicación de los checkpoints

Por defecto (configuración `etl.checkpoints.mode: auto` en `config.yaml`), los archivos se guardan en:


---

### 🗑️ Cómo borrar los checkpoints (para reprocesar todo o parte del ETL)

```bash
# Borrar todos los checkpoints (nodos y relaciones)
rm -rf .etl_checkpoints/

# Solo nodos:
rm -f .etl_checkpoints/nodes_hashbuck_*._DONE

# Solo relaciones:
rm -f .etl_checkpoints/rels_srcbuck_*._DONE
```
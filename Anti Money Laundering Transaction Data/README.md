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

# O bien, si son necesarias algunas configuraciones extra... (cambiar separador "," por ":" según si se utiliza Windows o Ubuntu)

# (3.1) Con los jars en las carpetas correspondientes (chequear ETL_config.yaml) -> use_packages: false y las líneas de los jars comentadas
/opt/spark/bin/spark-submit \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=6g \
  --conf spark.memory.fraction=0.6 \  
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.shuffle.partitions=32 \
  --conf spark.sql.adaptive.enabled=true \  
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.task.maxFailures=1 \
  --conf spark.stage.maxConsecutiveAttempts=1 \
  --conf spark.speculation=false \
  --conf spark.ui.showConsoleProgress=false \
  --conf "spark.driver.extraJavaOptions=-Dlog4j2.configurationFile=config/log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j2.configurationFile=config/log4j2.properties" \
  --conf spark.driver.extraClassPath=/opt/spark/jars/postgresql-42.7.4.jar:/opt/spark/jars/neo4j-connector-apache-spark_2.12-5.3.10_for_spark_3.jar \
  --conf spark.executor.extraClassPath=/opt/spark/jars/postgresql-42.7.4.jar:/opt/spark/jars/neo4j-connector-apache-spark_2.12-5.3.10_for_spark_3.jar \
  "3 - ETL2Neo4j.py"

# (3.2) Utilizando los packages: ETL_config.yaml -> use_packages: true
/opt/spark/bin/spark-submit \
  --packages org.postgresql:postgresql:42.7.4,org.neo4j:neo4j-connector-apache-spark_2.13:5.3.3_for_spark_4 \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=6g \
  --conf spark.memory.fraction=0.6 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.shuffle.partitions=32 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.task.maxFailures=1 \
  --conf spark.stage.maxConsecutiveAttempts=1 \
  --conf spark.speculation=false \
  --conf spark.ui.showConsoleProgress=false \
  --conf "spark.driver.extraJavaOptions=-Dlog4j2.configurationFile=config/log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j2.configurationFile=config/log4j2.properties" \
  "3 - ETL2Neo4j.py"

# (3.3) Declarando directamente el uso de los jars: ETL_config.yaml -> use_packages: false, y las líneas no comentadas
/opt/spark/bin/spark-submit \
  --jars /opt/spark/jars/postgresql-42.7.4.jar,/opt/spark/jars/neo4j-connector-apache-spark_2.13-5.3.3_for_spark_4.jar \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=6g \
  --conf spark.memory.fraction=0.6 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.shuffle.partitions=32 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.task.maxFailures=1 \
  --conf spark.stage.maxConsecutiveAttempts=1 \
  --conf spark.speculation=false \
  --conf spark.ui.showConsoleProgress=false \
  --conf "spark.driver.extraJavaOptions=-Dlog4j2.configurationFile=config/log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j2.configurationFile=config/log4j2.properties" \
  "3 - ETL2Neo4j.py"

#(3.4) Declarando indirectamente el uso de los jars: ETL_config.yaml -> use_packages: false, y las líneas no comentadas
/opt/spark/bin/spark-submit \  
  --conf spark.driver.extraClassPath=/opt/spark/jars/postgresql-42.7.4.jar:/opt/spark/jars/neo4j-connector-apache-spark_2.12-5.3.10_for_spark_3.jar \
  --conf spark.executor.extraClassPath=/opt/spark/jars/postgresql-42.7.4.jar:/opt/spark/jars/neo4j-connector-apache-spark_2.12-5.3.10_for_spark_3.jar \  
  --conf spark.driver.memory=8g \  
  --conf spark.executor.memory=6g \
  --conf spark.memory.fraction=0.6 \  
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.shuffle.partitions=32 \  
  --conf spark.sql.adaptive.enabled=true \  
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.task.maxFailures=1 \  
  --conf spark.stage.maxConsecutiveAttempts=1 \
  --conf spark.speculation=false \
  --conf spark.ui.showConsoleProgress=false \
  --conf "spark.driver.extraJavaOptions=-Dlog4j2.configurationFile=config/log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j2.configurationFile=config/log4j2.properties" \   
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

## 6) 🧾 Notas finales

### 🔧 Configuración
- Edita rutas en `config.yaml` si cambias de equipo o disco.  
- El archivo `config.yaml` ya optimiza `spark.shuffle.partitions` y usa `MEMORY_AND_DISK` para evitar desbordes de memoria.  

### 💡 Compatibilidad
- Neo4j usa **Java 21**; Spark 4.0.1 usa **Java 17** → asegúrate de que ambos estén correctamente configurados en tu entorno (puede requerir `update-alternatives` o exportar `JAVA_HOME` apropiado).  
- La interfaz web de Neo4j (`http://localhost:7474/browser`) no afecta la conexión `bolt://localhost:7687`.

### 📦 Dependencias
- Verifica que los JARs declarados en `ETL_config.yaml` correspondan a **Spark 4 / Scala 2.13** (Windows) o a **Spark 3 / Scala 2.12** (Ubuntu):
  - `org.postgresql:postgresql:42.7.4`
  - `org.neo4j:neo4j-connector-apache-spark_2.13:5.3.x_for_spark_4` # O
  - `org.neo4j:neo4j-connector-apache-spark_2.13:5.3.x_for_spark_3`

---

## 7) ⚠️ Checkpoints del ETL (control de reintentos por bucket)

El script **`3 - ETL2Neo4j.py`** implementa un sistema de checkpoints que permite **reanudar cargas interrumpidas sin duplicar datos**.  
Cada bucket procesado genera un archivo `._DONE` dentro del directorio `.etl_checkpoints/`.

Al reejecutar el pipeline, los buckets marcados se omiten automáticamente, mostrando mensajes como:


```bash
[NODOS] Skip bucket 0 (...)
[NODOS] Skip bucket 1 (...)
...
```

y no volverá a cargar esos datos.

### 📁 Ubicación de los checkpoints

Por defecto (configuración `etl.checkpoints.mode: auto` en `config.yaml`), los archivos se guardan en:

> 💡 **Sugerencia:** mantener los checkpoints activados en entornos productivos y desactivarlos solo durante pruebas o depuración.

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

### 🧩 Propiedades de los Nodos (`:Account`)

| Propiedad | Tipo / Ejemplo | Fuente | Descripción | ¿Recomendado? |
|------------|----------------|--------|--------------|----------------|
| **account_number** | string (`"AC12345"`) | accounts.account | Identificador único de la cuenta. | ✅ Esencial (clave primaria en Neo4j). |
| **location** | string (`"US"`, `"DE"`) | accounts.location | País/ubicación dominante de la cuenta. | ✅ Útil para segmentación geográfica. |
| **first_seen** | timestamp (`2023-01-02 10:00:00`) | statements.date_time (mínimo) | Fecha del primer movimiento registrado. | ✅ Importante para análisis temporal. |
| **last_seen** | timestamp (`2024-10-05 15:45:00`) | statements.date_time (máximo) | Fecha del último movimiento registrado. | ✅ Muy útil. |
| **current_balance** | decimal(18,2) | statements.running_balance del último registro | Saldo actual estimado según los movimientos. | ✅ Central para nodos financieros. |
| **mclose_YYYY_MM_CUR** | decimal(18,2) | statements (último del mes) | Cierre mensual por moneda. | ⚙️ Opcional — bueno si haces análisis histórico o visualización temporal. |
| **num_tx** | int | COUNT(statements) | Cantidad total de movimientos de la cuenta. | ⚙️ Recomendado (barato de calcular, muy útil). |
| **total_in / total_out** | decimal | SUM(delta>0) / SUM(delta<0) | Montos totales recibidos y enviados. | ⚙️ Opcional — simplifica consultas de flujo. |

### 🔗 Propiedades de las Aristas (`:TX`)

| Propiedad | Tipo / Ejemplo | Fuente | Descripción | ¿Recomendado? |
|------------|----------------|--------|--------------|----------------|
| **txn_id** | string | transfers.id | Identificador único de la transacción. | ✅ Esencial (PK de la relación). |
| **date_time** | timestamp | transfers.date_time | Momento en que ocurrió la transacción. | ✅ Fundamental para análisis temporal. |
| **amount** | decimal(18,2) | transfers.amount | Monto transferido (positivo absoluto). | ✅ Central. |
| **currency** | string | derivado de payment_currency / received_currency | Moneda de la transacción. | ✅ Importante para balances multimoneda. |
| **src_balance_before** | decimal | statements.running_balance - delta | Saldo del emisor antes del movimiento. | ⚙️ Útil si necesitas trazabilidad de saldos. |
| **src_balance_after** | decimal | statements.running_balance | Saldo del emisor tras la transacción. | ⚙️ Igual que arriba, opcional. |
| **dst_balance_before** | decimal | idem receptor | Saldo del receptor antes de recibir. | ⚙️ Igual. |
| **dst_balance_after** | decimal | idem receptor | Saldo del receptor después de recibir. | ⚙️ Igual. |
| **src_seq / dst_seq** | int | row_number() por cuenta | Orden secuencial del movimiento en cada cuenta. | ⚙️ Opcional — útil si consultas trayectorias temporales. |
| **masked** | boolean | Bernoulli(0.2) aleatorio | Marcador de anonimización o muestreo. | ❌ Probablemente experimental; puedes omitirlo. |

1 - ingesta_saml_d.py

Ingresa los datos del CSV original del dataset Kaggle a PostgreSQL.

Usa Spark con JDBC, crea una tabla “raw” y normaliza columnas.

2️⃣ 2 - internalETL.py

Lee la tabla “raw” y genera tres tablas limpias y derivadas:

accounts

transferences

statements

Añade llaves primarias y relaciones consistentes.

Escribe los resultados de nuevo a PostgreSQL (en schema saml_d).

3️⃣ 3 - ETL2Neo4j.py

Fase final del pipeline: Postgres → Neo4j.

Crea sesión Spark con configuraciones del módulo spark_session.

Usa módulos del paquete ETL_pg2neo4j para:

Leer datos de Postgres (io_pg.py)

Transformar a nodos/aristas (transform.py)

Ingestar en Neo4j (io_neo4j.py)

Registrar logs y checkpoints (logs.py, utils.py)

Controlar paralelismo y recursos (diagnostics.py)

Es un orquestador: ejecuta todo el flujo end-to-end.

## 🧠 Estructura modular del paquete `ETL_pg2neo4j`

| 🧩 Módulo | 📝 Función principal |
|-----------|----------------------|
| `load_config.py` | 🔧 Configuración y resolución de rutas. |
| `spark_session.py` | 🚀 Inicialización de Spark con jars/paquetes. |
| `io_pg.py` | 💾 Lectura particionada desde Postgres. |
| `transform.py` | 🔄 Construcción de nodos y relaciones enriquecidas. |
| `io_neo4j.py` | 🕸️ Ingesta a Neo4j con batching y checkpoints. |
| `diagnostics.py` | 🧠 Ajustes de rendimiento y planificación. |
| `logs.py` | 📜 Gestión estructurada de logs (Spark/Python). |
| `utils.py` | 🧰 Utilidades varias (checkpoints, ETA, StayAwake). |


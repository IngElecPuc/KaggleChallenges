with open("config.yaml", "r") as f:
    CFG = yaml.safe_load(f)

IS_WIN          = platform.system() == "Windows"
SPARK_LOCAL_DIR = CFG["spark"]["local_dirs"]["windows" if IS_WIN else "linux"]
PG_URL          = CFG["postgres"]["url"]
PG_USER         = CFG["postgres"]["user"]
PG_PASS         = CFG["postgres"]["pass"]
PG_SCHEMA       = CFG["postgres"]["schema_out"]["schema_name"]
PG_TABLE1       = CFG["postgres"]["schema_out"]["table1"]
PG_TABLE2       = CFG["postgres"]["schema_out"]["table2"]
PG_TABLE3       = CFG["postgres"]["schema_out"]["table3"]
JDBC_BATCHSIZE  = CFG["postgres"]["batchsize"]
JDBC_FETCHSIZE  = CFG["postgres"]["fetchsize"]
NEO4J_URI       = CFG["neo4j"]["uri"]
NEO4J_USER      = CFG["neo4j"]["user"]
NEO4J_PASS      = CFG["neo4j"]["pass"]
NEO4J_DDBB      = CFG["neo4j"]["database"]

PYTHON = sys.executable  # python del kernel Jupyter

def _in_notebook() -> bool:
    try:
        from IPython import get_ipython  # noqa
        return "IPKernelApp" in get_ipython().config
    except Exception:
        return False

def resolve_checkpoint_dir(cfg) -> Path:
    opt = cfg["etl"]["checkpoints"]
    enabled = bool(opt.get("enabled", True))
    if not enabled:
        return None

    mode = (opt.get("mode") or "auto").lower()
    subdir = opt.get("subdir") or ".etl_checkpoints"

    # 1) base_dir según modo
    if mode == "cwd":
        base = Path.cwd()
    elif mode == "script":
        # Puede fallar en notebook (no hay __file__)
        base = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
    elif mode == "custom":
        raw = opt.get("custom_dir") or ""
        base = Path(raw).expanduser().resolve() if raw else Path.cwd()
    else:  # "auto"
        if _in_notebook():
            base = Path.cwd()
        else:
            base = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()

    chk = (base / subdir).resolve()
    chk.mkdir(parents=True, exist_ok=True)
    return chk

CHK_DIR = resolve_checkpoint_dir(CFG)  # Path | None
print(f"Checkpoint dir: {CHK_DIR if CHK_DIR else 'DISABLED'}")

def _flag_path(prefix: str, bucket: int):
    if CHK_DIR is None:
        return None
    return (CHK_DIR / f"{prefix}_{bucket}._DONE").resolve()

def was_done(prefix: str, bucket: int) -> bool:
    p = _flag_path(prefix, bucket)
    return p.exists() if p else False

def mark_done(prefix: str, bucket: int) -> None:
    p = _flag_path(prefix, bucket)
    if p:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.touch()

builder = (SparkSession.builder
           .appName("postgres-to-neo4j-graph")
           .master(f"local[{usable_cores}]") #Limitar el uso de workers a n-1
           .config("spark.pyspark.driver.python", PYTHON)
           .config("spark.pyspark.python", PYTHON)
           .config("spark.executorEnv.PYSPARK_PYTHON", PYTHON)
           .config("spark.driver.bindAddress", "127.0.0.1")
           .config("spark.driver.host", "127.0.0.1")
           .config("spark.python.use.daemon", "false")
           .config("spark.local.dir", SPARK_LOCAL_DIR)
           .config("spark.sql.shuffle.partitions", str(CFG["spark"]["shuffle_partitions"]))
           .config("spark.driver.memory", CFG["spark"]["driver_memory"])
           .config("spark.sql.execution.arrow.pyspark.enabled", "true")
           #.config("spark.jars.packages", ",".join(CFG["spark"]["maven_packages"]))
          )
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

jdbc_props = {
    "user": PG_USER,
    "password": PG_PASS,
    "driver": "org.postgresql.Driver",
    "fetchsize": str(JDBC_FETCHSIZE)
}
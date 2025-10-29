import yaml, sys, platform
from pathlib import Path

with open("config/ETL_config.yaml", "r") as f:
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
PG_SCHEMA_LOG   = CFG["postgres"]["schema_etl"]["schema_name"]
PG_TABLE_LOG    = CFG["postgres"]["schema_etl"]["table1"]
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

__all__ = [
    "CFG","IS_WIN","SPARK_LOCAL_DIR",
    "PG_URL","PG_USER","PG_PASS","PG_SCHEMA","PG_TABLE1","PG_TABLE2","PG_TABLE3",
    "JDBC_BATCHSIZE","JDBC_FETCHSIZE",
    "NEO4J_URI","NEO4J_USER","NEO4J_PASS","NEO4J_DDBB",
    "PYTHON","CHK_DIR"
]

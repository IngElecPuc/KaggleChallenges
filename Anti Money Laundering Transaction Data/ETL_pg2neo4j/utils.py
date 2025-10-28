import ctypes, subprocess, atexit, signal
import os, time, platform
from datetime import timedelta
from ETL_pg2neo4j.load_config import CHK_DIR

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

def count_df(df):
    # cuenta y devuelve int
    return df.count()

def estimate_eta(done, total, start_ts):
    now = time.time()
    elapsed = now - start_ts
    rate = done / elapsed if done > 0 else 0.0
    remaining = total - done
    eta_s = (remaining / rate) if rate > 0 else float("inf")
    pct = (done / total * 100.0) if total else 0.0
    return pct, timedelta(seconds=int(eta_s)), timedelta(seconds=int(elapsed))

class StayAwake:
    """
    Mantiene el sistema despierto mientras el contexto está activo.
    - Windows: SetThreadExecutionState
    - Linux: systemd-inhibit con un proceso bloqueante
    """
    def __init__(self, reason: str = "Spark ETL running"):
        self.reason = reason
        self.proc = None
        self.is_win = platform.system() == "Windows"

    def __enter__(self):
        if self.is_win:
            # ES_CONTINUOUS(0x80000000) | ES_SYSTEM_REQUIRED(0x00000001) | ES_AWAYMODE_REQUIRED(0x00000040)
            self._win_set(True)
            atexit.register(self._win_set, False)
        else:
            # Lanza un proceso que se queda bloqueado hasta que salgamos
            # --what=sleep:idle evita suspensión e inactividad
            self.proc = subprocess.Popen(
                ["systemd-inhibit", "--what=sleep:idle", f"--why={self.reason}",
                 "--mode=block", "tail", "-f", "/dev/null"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, preexec_fn=os.setsid
            )
            atexit.register(self._linux_stop)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.is_win:
            self._win_set(False)
        else:
            self._linux_stop()

    def _win_set(self, enable: bool):
        ES_CONTINUOUS = 0x80000000
        ES_SYSTEM_REQUIRED = 0x00000001
        ES_AWAYMODE_REQUIRED = 0x00000040
        val = ES_CONTINUOUS
        if enable:
            val |= (ES_SYSTEM_REQUIRED | ES_AWAYMODE_REQUIRED)
        ctypes.windll.kernel32.SetThreadExecutionState(val)

    def _linux_stop(self):
        if self.proc and self.proc.poll() is None:
            try:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
            except Exception:
                pass             
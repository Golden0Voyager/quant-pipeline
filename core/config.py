"""
管线配置模块
────────────
提供 PipelineConfig dataclass，集中管理所有环境变量和默认值。
替代 daily_pipeline.py 中的全局常量。
"""

from __future__ import annotations

import logging
import os
import socket
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# 兄弟仓库（`smartmoney_hunter`）的路径由 `core/_bootstrap.py` 统一注入，并挂在
# `core/__init__.py` 上——所以 `import core.config` 时它已经就位。不要再在这里内联
# `sys.path` 操作：那样只覆盖「恰好先导入本模块」的调用方。

logger = logging.getLogger(__name__)


def load_env_file(env_path: str | Path = ".env") -> None:
    """
    从项目根目录加载 .env 文件到环境变量。
    格式：KEY=VALUE，支持 # 注释和空行。
    """
    env_file = Path(env_path)
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip().strip("\"'")
        if key not in os.environ:  # 不覆盖已存在的环境变量
            os.environ[key] = value


# 启动时自动加载 .env
load_env_file()


def _detect_macos_proxy() -> None:
    """检测 macOS 系统代理设置并注入环境变量。"""
    if os.getenv("HTTP_PROXY") or os.getenv("http_proxy"):
        return
    try:
        _proxy_out = subprocess.run(
            ["scutil", "--proxy"], capture_output=True, text=True, timeout=5
        )
        if "HTTPEnable : 1" in _proxy_out.stdout and "HTTPProxy" in _proxy_out.stdout:
            _host = _port = None
            for _line in _proxy_out.stdout.split("\n"):
                _line = _line.strip()
                if _line.startswith("HTTPProxy"):
                    _host = _line.split(":")[1].strip()
                elif _line.startswith("HTTPPort"):
                    _port = _line.split(":")[1].strip()
            if _host and _port:
                _proxy_url = f"http://{_host}:{_port}"
                os.environ["HTTP_PROXY"] = _proxy_url
                os.environ["HTTPS_PROXY"] = _proxy_url
                os.environ["http_proxy"] = _proxy_url
                os.environ["https_proxy"] = _proxy_url
                # 境内数据源全部直连：系统代理指向的本机代理若已关闭，
                # 走代理的境内请求会以 ProxyError / SSL EOF 失败
                #（urllib 的 no_proxy 按 endswith 匹配，用裸域名后缀）
                _no_proxy = (
                    "localhost,127.0.0.1,"
                    "eastmoney.com,"
                    "sina.com,sina.cn,sina.com.cn,"
                    "sse.com.cn,szse.cn,"
                    "jin10.com,csindex.com.cn,cninfo.com.cn"
                )
                os.environ["NO_PROXY"] = _no_proxy
                os.environ["no_proxy"] = _no_proxy
    except Exception:
        pass


# ---------------------------------------------------------------------------
# 配置：数据库路径（环境变量优先）
# ---------------------------------------------------------------------------
DEFAULT_DB_PATH = os.path.expanduser("~/Code/quant_data/quant_core.db")
DB_PATH = os.getenv("QUANT_DB_PATH", DEFAULT_DB_PATH)
SHARED_DATA_DIR = Path(DB_PATH).parent

# 全量拉取回溯天数（默认 2190 天 ≈ 6 年）
DEFAULT_LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "2190"))

# ---------------------------------------------------------------------------
# 收盘刷新跨源抽样校验（--refresh-today，默认关闭）
# ---------------------------------------------------------------------------
# 默认值可为模块常量，但环境变量必须在调用时读取（见 read_cross_source_config），
# 避免 import 期固化导致测试无法通过 monkeypatch 翻转开关。
CROSS_SOURCE_TASK_DEFAULT = "update_bars"
CROSS_SOURCE_SAMPLE_SIZE_DEFAULT = 30
# 注意：以下容差为临时值，必须先用真实数据调参后才可信任该校验结果（后续第 3 步）。
CROSS_SOURCE_PRICE_TOL_DEFAULT = 0.005
CROSS_SOURCE_VOLUME_TOL_DEFAULT = 0.05


@dataclass(frozen=True)
class CrossSourceEnvConfig:
    """从环境变量读取的跨源校验配置快照。"""

    enabled: bool
    task_name: str
    sample_size: int
    price_tol: float
    volume_tol: float
    report_only: bool


def read_cross_source_config() -> CrossSourceEnvConfig:
    """每次调用即时读取环境变量（与 INCLUDE_BJ 相同的布尔约定）。"""
    return CrossSourceEnvConfig(
        enabled=os.getenv("REFRESH_CROSS_SOURCE", "0").lower() in ("1", "true", "yes"),
        task_name=os.getenv("REFRESH_CROSS_SOURCE_TASK", CROSS_SOURCE_TASK_DEFAULT),
        sample_size=int(
            os.getenv(
                "REFRESH_CROSS_SOURCE_SAMPLE_SIZE", str(CROSS_SOURCE_SAMPLE_SIZE_DEFAULT)
            )
        ),
        price_tol=float(
            os.getenv(
                "REFRESH_CROSS_SOURCE_PRICE_TOL", str(CROSS_SOURCE_PRICE_TOL_DEFAULT)
            )
        ),
        volume_tol=float(
            os.getenv(
                "REFRESH_CROSS_SOURCE_VOLUME_TOL", str(CROSS_SOURCE_VOLUME_TOL_DEFAULT)
            )
        ),
        # 默认 "1"：首次启用跨源校验即从观察模式起步（只记录不降级），
        # 需显式置 0 才切换为 enforce（定向重试 + 降级）。
        report_only=os.getenv("REFRESH_CROSS_SOURCE_REPORT_ONLY", "1").lower()
        in ("1", "true", "yes"),
    )


# ---------------------------------------------------------------------------
# 日志配置（统一存放到数据目录下）
# ---------------------------------------------------------------------------
LOG_DIR = SHARED_DATA_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"smartmoney_{datetime.now().strftime('%Y%m%d')}.log"

# 强制配置根 logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
for h in root_logger.handlers[:]:
    root_logger.removeHandler(h)

_file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
_file_handler.setFormatter(
    logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s")
)
root_logger.addHandler(_file_handler)

if sys.stdout.isatty():
    _console_handler = logging.StreamHandler(sys.stdout)
    _console_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s")
    )
    root_logger.addHandler(_console_handler)


# ---------------------------------------------------------------------------
# 全局套接字超时
# ---------------------------------------------------------------------------
socket.setdefaulttimeout(15)

# macOS 代理注入
_detect_macos_proxy()

# ---------------------------------------------------------------------------
# PipelineConfig：类型化配置
# ---------------------------------------------------------------------------

BATCH_SIZE_VAL = 100
BATCH_SLEEP_VAL = 5.0
PER_STOCK_MIN_SLEEP_VAL = 0.5
PER_STOCK_MAX_SLEEP_VAL = 1.2
MAX_RETRY_VAL = 3
RETRY_DELAY_VAL = 6.0
PROGRESS_FLUSH_INTERVAL_VAL = 10
# 默认 3：与生产 .env 历史值一致，作用范围仅 stage4（12 任务写表不相交）与
# bars 内部批次池；stage2/3/5 的串行是编排层硬编码，不随此值变化。
PARALLEL_WORKERS_VAL = int(os.getenv("PARALLEL_WORKERS", "3"))
CHIP_BINS_VAL = int(os.getenv("CHIP_BINS", "100"))
MIN_CHIP_DAYS_VAL = int(os.getenv("MIN_CHIP_DAYS", "60"))
CHIP_MAX_TURNOVER_VAL = 0.999
MIN_FUNDAMENTALS_STOCK_COUNT_VAL = 5000

# ULTRA_SAFE 模式改写
if os.getenv("ULTRA_SAFE") == "1":
    BATCH_SIZE_VAL = 30
    BATCH_SLEEP_VAL = 12.0
    PER_STOCK_MIN_SLEEP_VAL = 0.8
    PER_STOCK_MAX_SLEEP_VAL = 2.0
    MAX_RETRY_VAL = 2
    RETRY_DELAY_VAL = 3.0
    # ULTRA_SAFE 语义是全链路减压：并发度也必须回到最保守值，
    # 否则默认改为 3 后该模式会被动获得并发。
    PARALLEL_WORKERS_VAL = 1


@dataclass
class PipelineConfig:
    """类型化配置，集中管理所有环境变量和运行参数。"""

    # 数据库路径
    db_path: str = field(default_factory=lambda: DB_PATH)
    shared_data_dir: Path = field(default_factory=lambda: SHARED_DATA_DIR)
    log_dir: Path = field(default_factory=lambda: LOG_DIR)

    # 回溯天数
    lookback_days: int = field(default_factory=lambda: DEFAULT_LOOKBACK_DAYS)

    # 股票过滤
    include_bj: bool = field(
        default_factory=lambda: os.getenv("INCLUDE_BJ", "0").lower() in ("1", "true", "yes")
    )
    ultra_safe: bool = field(
        default_factory=lambda: os.getenv("ULTRA_SAFE") == "1"
    )

    # 运行参数
    parallel_workers: int = field(default_factory=lambda: PARALLEL_WORKERS_VAL)
    batch_size: int = field(default_factory=lambda: BATCH_SIZE_VAL)
    batch_sleep: float = field(default_factory=lambda: BATCH_SLEEP_VAL)
    per_stock_min_sleep: float = field(default_factory=lambda: PER_STOCK_MIN_SLEEP_VAL)
    per_stock_max_sleep: float = field(default_factory=lambda: PER_STOCK_MAX_SLEEP_VAL)
    max_retry: int = field(default_factory=lambda: MAX_RETRY_VAL)
    retry_delay: float = field(default_factory=lambda: RETRY_DELAY_VAL)
    progress_flush_interval: int = field(default_factory=lambda: PROGRESS_FLUSH_INTERVAL_VAL)

    # 筹码分布参数
    chip_bins: int = field(default_factory=lambda: CHIP_BINS_VAL)
    min_chip_days: int = field(default_factory=lambda: MIN_CHIP_DAYS_VAL)
    chip_max_turnover: float = field(default_factory=lambda: CHIP_MAX_TURNOVER_VAL)

    # 其他阈值
    min_fundamentals_stock_count: int = field(
        default_factory=lambda: MIN_FUNDAMENTALS_STOCK_COUNT_VAL
    )

    @classmethod
    def from_env(cls) -> PipelineConfig:
        """从环境变量创建配置实例。"""
        return cls()

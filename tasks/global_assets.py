import logging
from typing import Any
import pandas as pd
from datetime import datetime

from core.task_result import TaskResult
from core.task_registry import TASK_REGISTRY, Cadence
from interface import ProviderFactory

logger = logging.getLogger(__name__)

# 全球核心资产预设列表 (含科技巨头、半导体、医药、消费与加密货币)
GLOBAL_ASSETS = [
    # 科技与创新
    "AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMZN", "META", "NFLX", "AMD", "SPCX",
    # 芯片半导体
    "INTC", "QCOM", "AVGO", "TSM", "ASML",
    # 医药与创新药
    "LLY", "NVO", "JNJ", "MRK", "ABBV", "PFE", "AMGN", "VRTX", "REGN",
    # 日常消费与零售
    "WMT", "PG", "KO", "PEP", "COST", "MCD", "SBUX", "NKE", "HD",
    # 加密货币
    "BTC-USD"
]

def update_global_assets(*args: Any, **kwargs: Any) -> TaskResult:
    """拉取预设的全球核心资产（如美股科技、BTC）历史/增量数据并存入 global_assets_bars 表。"""
    
    db = ProviderFactory.get_db()
    loader = ProviderFactory.get_loader()
    
    success_count = 0
    failed_symbols = []
    
    for symbol in GLOBAL_ASSETS:
        try:
            # 获取最近 2 年的数据，利用 INSERT OR REPLACE 幂等写入
            start_dt = (datetime.now() - pd.Timedelta(days=730)).strftime("%Y-%m-%d")
            
            # 使用我们在 DataLoaderInterface 中新加的方法
            df = loader.fetch_global_assets_bars(symbol, start_date=start_dt)
            if df is None or df.empty:
                logger.warning(f"  [{symbol}] 无数据返回")
                failed_symbols.append(symbol)
                continue
            
            records = df.to_dict(orient="records")
            # 使用我们在 DatabaseInterface 中新加的方法
            saved = db.save_global_assets_bars_batch(records)
            logger.info(f"  [{symbol}] 成功更新 {saved} 条全球资产数据")
            success_count += 1
            
        except Exception as e:
            logger.exception(f"更新全球核心资产失败 {symbol}: {e}")
            failed_symbols.append(symbol)
            
    if failed_symbols:
        from core.task_result import ErrorKind
        return TaskResult.degraded(
            task_name="update_global_assets",
            error_kind=ErrorKind.INTERNAL,
            error=f"部分全球资产抓取失败: {failed_symbols}",
            fetched=len(GLOBAL_ASSETS),
            saved=success_count
        )
        
    return TaskResult.success(
        task_name="update_global_assets",
        fetched=len(GLOBAL_ASSETS),
        saved=success_count
    )

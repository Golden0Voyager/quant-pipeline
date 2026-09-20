import logging
import time
from datetime import datetime
from typing import Any

import pandas as pd

from core.task_result import ErrorKind, TaskResult
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
    "BTC-USD",
    # 宏观与市场情绪（美联储加息周期核心传导变量）
    "^VIX",            # 恐慌指数（现货）
    "^VIX9D",          # VIX 近端（与 ^VIX3M 组成期限结构，contango/backwardation 比点位更有预测力）
    "^VIX3M",          # VIX 3 个月
    "^MOVE",           # 美债波动率（债市定价动荡，加息周期核心仪表）
    "USDCNH=X",        # 离岸人民币（在岸 fx_rate 表为中行牌价）
    "DX-Y.NYB",        # 美元指数（akshare global_index 对其覆盖率仅 13/50 交易日，不可靠）
    "^HSI",            # 恒生指数（同上，东财/新浪源均间歇性缺失）
    # 板块权益确认（与商品/芯片现货背离时是趋势预警）
    "XLE",             # 能源精选行业 ETF（原油趋势权益端验证）
    "OIH",             # 石油服务 ETF
    "^SOX",            # 费城半导体指数（与 A 股芯片板块强联动）
    # 大宗商品日线（akshare 原油接口仅约 2 个月盘中快照、无成交量；
    # 黄金 SGE 接口虽全量但每日重复存历史，均不适合直接做序列分析，此处补官方日线）
    "CL=F",            # WTI 原油期货
    "BZ=F",            # Brent 原油期货
    "GC=F",            # COMEX 黄金
    "SI=F",            # COMEX 白银
]

# 增量拉取的重叠窗口：从库内最新日前回退几天续拉（INSERT OR REPLACE 幂等），
# 覆盖周末/美股节假日等「暂无新数据」的正常情形，避免空响应被误判为失败
_INCREMENTAL_OVERLAP_DAYS = 5
# 新股/库内无数据时的全量回填窗口
_FULL_LOOKBACK_DAYS = 730
# yfinance 限流常假报 "possibly delisted; no price data found"
# （2026-09-15 JNJ/MRK），空响应后重试一次
_FETCH_RETRY_DELAY = 10.0
# 高成功率容忍：个别 symbol 限流/缺数不拖垮整个管道（degraded 会被
# 管道汇总判为失败导致 exit 1），失败清单保留在日志与 metadata 中
_SUCCESS_TOLERANCE = 0.9


def _fetch_with_retry(loader: Any, symbol: str, start_dt: str) -> pd.DataFrame:
    """拉取单只资产数据，空响应重试一次。"""
    for attempt in range(2):
        df = loader.fetch_global_assets_bars(symbol, start_date=start_dt)
        if df is not None and not df.empty:
            return df
        if attempt == 0:
            logger.warning(f"  [{symbol}] 无数据返回，{_FETCH_RETRY_DELAY:.0f}s 后重试...")
            time.sleep(_FETCH_RETRY_DELAY)
    return pd.DataFrame()


def update_global_assets(*args: Any, **kwargs: Any) -> TaskResult:
    """拉取预设的全球核心资产（如美股科技、BTC）增量数据并存入 global_assets_bars 表。"""

    db = ProviderFactory.get_db()
    loader = ProviderFactory.get_loader()

    success_count = 0
    failed_symbols = []

    for symbol in GLOBAL_ASSETS:
        try:
            # 增量拉取：库内最新日前回退数天续拉（2026-09-15 前每天全量拉 730 天，
            # 既慢又放大 yfinance 限流概率）；库内无数据则全量回填
            latest = db.get_global_assets_latest_date(symbol)
            if latest:
                start_dt = (
                    datetime.strptime(latest, "%Y-%m-%d")
                    - pd.Timedelta(days=_INCREMENTAL_OVERLAP_DAYS)
                ).strftime("%Y-%m-%d")
            else:
                start_dt = (
                    datetime.now() - pd.Timedelta(days=_FULL_LOOKBACK_DAYS)
                ).strftime("%Y-%m-%d")

            df = _fetch_with_retry(loader, symbol, start_dt)
            if df.empty:
                logger.warning(f"  [{symbol}] 无数据返回")
                failed_symbols.append(symbol)
                continue

            records = df.to_dict(orient="records")
            saved = db.save_global_assets_bars_batch(records)
            logger.info(f"  [{symbol}] 成功更新 {saved} 条全球资产数据")
            success_count += 1

        except Exception as e:
            logger.exception(f"更新全球核心资产失败 {symbol}: {e}")
            failed_symbols.append(symbol)

    if failed_symbols and success_count < len(GLOBAL_ASSETS) * _SUCCESS_TOLERANCE:
        return TaskResult.degraded(
            task_name="update_global_assets",
            error_kind=ErrorKind.INTERNAL,
            error=f"部分全球资产抓取失败: {failed_symbols}",
            fetched=len(GLOBAL_ASSETS),
            saved=success_count
        )

    if failed_symbols:
        logger.warning(f"⚠️ 个别全球资产抓取失败（成功率达标，容忍）: {failed_symbols}")
    return TaskResult.success(
        task_name="update_global_assets",
        fetched=len(GLOBAL_ASSETS),
        saved=success_count,
        metadata={"failed_symbols": failed_symbols} if failed_symbols else None,
    )

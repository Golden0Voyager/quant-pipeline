-- Migration 026: market_breadth — 乐咕市场宽度（新高新低 / 破净 / 赚钱效应）
--
-- 数据源：乐咕乐咕 (legulegu.com)。
-- 一行一个交易日，三个来源按日期合并（high_low 全历史 + below_net_asset 全历史
-- + market_activity 当日快照）；缺失来源的列保持 NULL。

CREATE TABLE IF NOT EXISTS market_breadth (
    date DATE PRIMARY KEY,
    close REAL,                      -- high_low 接口返回的指数收盘（乐咕口径）
    high20 INTEGER,                  -- 20 日新高家数
    low20 INTEGER,
    high60 INTEGER,                  -- 60 日新高家数
    low60 INTEGER,
    high120 INTEGER,                 -- 120 日新高家数
    low120 INTEGER,
    below_net_asset INTEGER,         -- 破净股家数
    total_company INTEGER,           -- 全部 A 股家数（破净接口口径）
    below_net_asset_ratio REAL,      -- 破净占比
    up_count INTEGER,                -- 上涨家数（赚钱效应快照）
    down_count INTEGER,
    flat_count INTEGER,
    limit_up INTEGER,                -- 涨停（含 ST）
    limit_down INTEGER,
    real_limit_up INTEGER,           -- 真实涨停（非 ST）
    real_limit_down INTEGER,
    st_limit_up INTEGER,
    st_limit_down INTEGER,
    suspended INTEGER,               -- 停牌家数
    activity_ratio REAL,             -- 活跃度 (%)
    data_source TEXT DEFAULT 'legu',
    data_date TEXT,                  -- 本轮运行日期 (YYYY-MM-DD)
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_market_breadth_date
    ON market_breadth(date DESC);

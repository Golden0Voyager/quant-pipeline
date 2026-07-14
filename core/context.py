"""
任务上下文模块
──────────────
提供 TaskContext dataclass，封装 ProviderFactory 调用，
为并行任务创建独立 DB 连接。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from core.config import PipelineConfig
from interface import (
    DatabaseInterface,
    DataLoaderInterface,
    IndicatorEngineInterface,
    ProviderFactory,
)


@dataclass
class TaskContext:
    """
    任务上下文：封装任务执行所需的全部依赖。

    每个并行任务应使用 fresh_db() 获取独立的 DB 连接，
    避免 SQLite 连接在并发读写时产生 'database is locked' 错误。
    """

    config: PipelineConfig
    db: DatabaseInterface
    loader: DataLoaderInterface
    engine: IndicatorEngineInterface

    # 预留扩展字段
    extra: dict[str, Any] = field(default_factory=dict)

    def fresh_db(self) -> DatabaseInterface:
        """
        创建一个全新的 DatabaseInterface 实例。
        用于并行任务，避免共享 SQLite 连接。
        """
        ProviderFactory.configure(
            db_path=self.config.db_path,
            provider="smartmoney",
        )
        return ProviderFactory.get_db()


def create_context(
    db_path: str | None = None,
    config: PipelineConfig | None = None,
) -> TaskContext:
    """
    创建任务上下文的工厂函数。

    依据配置初始化 ProviderFactory，返回填充好的 TaskContext。
    """
    if config is None:
        config = PipelineConfig.from_env()
    ProviderFactory.configure(db_path=db_path or config.db_path, provider="smartmoney")
    return TaskContext(
        config=config,
        db=ProviderFactory.get_db(),
        loader=ProviderFactory.get_loader(),
        engine=ProviderFactory.get_indicator_engine(),
    )

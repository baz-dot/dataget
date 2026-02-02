"""
重试装饰器模块
支持指数退避、自定义异常处理
"""

import time
import functools
from dataclasses import dataclass, field
from typing import Callable, Tuple, Type, Optional, Any, List
import logging

from .logger import get_logger


@dataclass
class RetryConfig:
    """重试配置"""
    max_retries: int = 3
    base_delay: float = 1.0  # 基础延迟（秒）
    max_delay: float = 60.0  # 最大延迟（秒）
    exponential_base: float = 2.0  # 指数基数
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,)
    # 特定异常的延迟倍数（如 rate limit 需要更长等待）
    exception_delays: dict = field(default_factory=dict)


# 预定义配置
BIGQUERY_RETRY_CONFIG = RetryConfig(
    max_retries=3,
    base_delay=2.0,
    max_delay=30.0,
    exponential_base=2.0,
)

LARK_RETRY_CONFIG = RetryConfig(
    max_retries=3,
    base_delay=1.0,
    max_delay=15.0,
    exponential_base=2.0,
)

API_RETRY_CONFIG = RetryConfig(
    max_retries=5,
    base_delay=5.0,
    max_delay=60.0,
    exponential_base=2.0,
)


def retry_with_backoff(
    config: RetryConfig = None,
    max_retries: int = None,
    base_delay: float = None,
    logger_name: str = "dataget"
):
    """
    带指数退避的重试装饰器

    Args:
        config: RetryConfig 配置对象
        max_retries: 最大重试次数（覆盖 config）
        base_delay: 基础延迟（覆盖 config）
        logger_name: logger 名称

    Usage:
        @retry_with_backoff(config=BIGQUERY_RETRY_CONFIG)
        def upload_data():
            ...

        @retry_with_backoff(max_retries=3, base_delay=2.0)
        def call_api():
            ...
    """
    if config is None:
        config = RetryConfig()

    # 允许覆盖配置
    _max_retries = max_retries if max_retries is not None else config.max_retries
    _base_delay = base_delay if base_delay is not None else config.base_delay

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            logger = get_logger(logger_name)
            last_exception = None

            for attempt in range(_max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except config.retryable_exceptions as e:
                    last_exception = e

                    if attempt == _max_retries:
                        logger.error(
                            f"[{func.__name__}] 重试 {_max_retries} 次后仍失败: {e}"
                        )
                        raise

                    # 计算延迟时间（指数退避）
                    delay = min(
                        _base_delay * (config.exponential_base ** attempt),
                        config.max_delay
                    )

                    # 检查是否有特定异常的延迟配置
                    for exc_type, multiplier in config.exception_delays.items():
                        if isinstance(e, exc_type):
                            delay *= multiplier
                            break

                    logger.warning(
                        f"[{func.__name__}] 第 {attempt + 1} 次失败: {e}, "
                        f"{delay:.1f}s 后重试..."
                    )
                    time.sleep(delay)

            raise last_exception

        return wrapper
    return decorator

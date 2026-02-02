"""
统一日志模块
支持日志级别、文件持久化、结构化输出
"""

import logging
import os
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from typing import Optional


# 日志格式
DEFAULT_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DETAILED_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(filename)s:%(lineno)d | %(message)s"

# 默认日志目录
DEFAULT_LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")


def setup_logger(
    name: str = "dataget",
    level: int = logging.INFO,
    log_dir: Optional[str] = None,
    console_output: bool = True,
    file_output: bool = True,
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5,
    detailed: bool = False
) -> logging.Logger:
    """
    配置并返回 logger 实例

    Args:
        name: logger 名称
        level: 日志级别 (logging.DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_dir: 日志文件目录，默认为项目根目录下的 logs/
        console_output: 是否输出到控制台
        file_output: 是否输出到文件
        max_bytes: 单个日志文件最大字节数
        backup_count: 保留的日志文件数量
        detailed: 是否使用详细格式（包含文件名和行号）

    Returns:
        配置好的 logger 实例
    """
    logger = logging.getLogger(name)

    # 避免重复添加 handler
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # 选择日志格式
    log_format = DETAILED_FORMAT if detailed else DEFAULT_FORMAT
    formatter = logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S")

    # 控制台输出
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # 文件输出
    if file_output:
        if log_dir is None:
            log_dir = DEFAULT_LOG_DIR

        # 确保日志目录存在
        os.makedirs(log_dir, exist_ok=True)

        # 按大小轮转的日志文件
        log_file = os.path.join(log_dir, f"{name}.log")
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8"
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # 错误日志单独文件
        error_file = os.path.join(log_dir, f"{name}_error.log")
        error_handler = RotatingFileHandler(
            error_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8"
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        logger.addHandler(error_handler)

    return logger


# 缓存已创建的 logger
_loggers = {}


def get_logger(name: str = "dataget") -> logging.Logger:
    """
    获取 logger 实例（单例模式）

    Args:
        name: logger 名称

    Returns:
        logger 实例
    """
    if name not in _loggers:
        # 从环境变量读取配置
        level_str = os.getenv("LOG_LEVEL", "INFO").upper()
        level = getattr(logging, level_str, logging.INFO)

        log_dir = os.getenv("LOG_DIR", DEFAULT_LOG_DIR)
        console_output = os.getenv("LOG_CONSOLE", "true").lower() == "true"
        file_output = os.getenv("LOG_FILE", "true").lower() == "true"
        detailed = os.getenv("LOG_DETAILED", "false").lower() == "true"

        _loggers[name] = setup_logger(
            name=name,
            level=level,
            log_dir=log_dir,
            console_output=console_output,
            file_output=file_output,
            detailed=detailed
        )

    return _loggers[name]


# 便捷方法：获取子模块 logger
def get_module_logger(module_name: str) -> logging.Logger:
    """
    获取子模块的 logger

    Args:
        module_name: 模块名称，如 "bigquery", "lark"

    Returns:
        logger 实例
    """
    return get_logger(f"dataget.{module_name}")

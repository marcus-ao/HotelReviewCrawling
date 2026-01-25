"""日志配置模块"""
import sys
from pathlib import Path
from loguru import logger

from config.settings import settings


def setup_logger() -> None:
    """配置日志系统"""
    # 移除默认处理器
    logger.remove()

    # 日志格式
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )

    # 控制台输出
    logger.add(
        sys.stdout,
        format=log_format,
        level=settings.log_level,
        colorize=True,
    )

    # 确保日志目录存在
    log_path = settings.log_path
    log_path.mkdir(parents=True, exist_ok=True)

    # 常规日志文件（按天轮转）
    logger.add(
        log_path / "crawler_{time:YYYY-MM-DD}.log",
        format=log_format,
        level="DEBUG",
        rotation="00:00",
        retention="30 days",
        compression="zip",
        encoding="utf-8",
    )

    # 错误日志文件（单独记录）
    logger.add(
        log_path / "error_{time:YYYY-MM-DD}.log",
        format=log_format,
        level="ERROR",
        rotation="00:00",
        retention="60 days",
        compression="zip",
        encoding="utf-8",
    )

    logger.info("日志系统初始化完成")


def get_logger(name: str = None):
    """获取日志记录器

    Args:
        name: 模块名称，用于日志标识

    Returns:
        配置好的logger实例
    """
    if name:
        return logger.bind(name=name)
    return logger

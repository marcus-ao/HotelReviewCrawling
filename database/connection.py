"""数据库连接管理模块"""
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool

from config.settings import settings
from .models import Base


# 全局引擎实例
_engine = None
_SessionLocal = None


def get_engine():
    """获取数据库引擎（单例模式）"""
    global _engine
    if _engine is None:
        _engine = create_engine(
            settings.database_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
            echo=settings.log_level == "DEBUG",
        )
    return _engine


def get_session_factory():
    """获取会话工厂"""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            bind=get_engine(),
            autocommit=False,
            autoflush=False,
        )
    return _SessionLocal


def get_session() -> Session:
    """获取数据库会话"""
    SessionLocal = get_session_factory()
    return SessionLocal()


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    """提供事务范围的会话上下文管理器

    Usage:
        with session_scope() as session:
            session.add(obj)
            session.commit()
    """
    session = get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def close_session(session: Session) -> None:
    """关闭会话"""
    if session:
        session.close()


def init_db() -> None:
    """初始化数据库（创建所有表）"""
    engine = get_engine()
    Base.metadata.create_all(bind=engine)


def drop_all_tables() -> None:
    """删除所有表（谨慎使用）"""
    engine = get_engine()
    Base.metadata.drop_all(bind=engine)


def check_connection() -> bool:
    """检查数据库连接是否正常"""
    try:
        from sqlalchemy import text
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False

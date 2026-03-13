"""Database connection management for the metadata store (PostgreSQL)."""

from functools import lru_cache
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.config.settings import get_settings
from src.models.database import Base


@lru_cache()
def get_engine() -> Engine:
    """Create a cached SQLAlchemy engine from settings."""
    settings = get_settings()
    database_url = (
        f"postgresql://{settings.db_user}:{settings.db_password}"
        f"@{settings.db_host}:{settings.db_port}/{settings.db_name}"
    )
    return create_engine(
        database_url,
        pool_size=10,
        max_overflow=20,
        pool_recycle=3600,
        echo=False,
    )


def get_session() -> Generator[Session, None, None]:
    """Yield a database session (for use as FastAPI dependency)."""
    engine = get_engine()
    session_factory = sessionmaker(bind=engine)
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db() -> None:
    """Create all database tables (for development/testing)."""
    engine = get_engine()
    Base.metadata.create_all(bind=engine)

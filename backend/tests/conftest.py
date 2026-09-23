from __future__ import annotations

import io
import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("REPORTS_DIR", "./test_reports")

from app.db.database import Base, get_db  # noqa: E402
from app.main import app  # noqa: E402

TEST_DB_URL = "sqlite://"


@pytest.fixture(scope="function")
def db_session():
    engine = create_engine(
        TEST_DB_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    TestingSessionLocal = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
    )
    session = TestingSessionLocal()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(bind=engine)
        engine.dispose()
        


@pytest.fixture(scope="function")
def client(db_session):
    def _override_get_db():
        try:
            yield db_session
        finally:
            pass

    app.dependency_overrides[get_db] = _override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def make_csv_file(content: str, filename: str = "data.csv"):
    return (filename, io.BytesIO(content.encode("utf-8")), "text/csv")


GOOD_CSV = (
    "order_id,customer_id,order_amount,region\n"
    "1,C001,120.5,North\n"
    "2,C002,98.2,South\n"
    "3,C003,143.1,East\n"
    "4,C004,110.0,West\n"
)

BASELINE_CSV = (
    "order_id,customer_id,order_amount,region\n"
    "1,C001,119.0,North\n"
    "2,C002,97.0,South\n"
    "3,C003,140.0,East\n"
    "4,C004,111.0,West\n"
)

DRIFTED_CSV = (
    "order_id,customer_id,order_amount,discount_code\n"
    "1,C001,120.5,D1\n"
    "2,,98.2,D1\n"
    "3,,,D2\n"
    "4,C004,abc,D2\n"
)

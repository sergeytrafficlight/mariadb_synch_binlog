import pytest
import pymysql
import threading
import time
from unittest.mock import MagicMock
from config.config_test import MYSQL_SETTINGS, APP_SETTINGS, MYSQL_SETTINGS_ACTOR
from src.engine import run


@pytest.fixture(autouse=True)
def use_test_config(monkeypatch):
    import config.config as prod_cfg
    monkeypatch.setattr(prod_cfg, "MYSQL_SETTINGS", MYSQL_SETTINGS)
    monkeypatch.setattr(prod_cfg, "APP_SETTINGS", APP_SETTINGS)

@pytest.fixture
def fake_cursor():
    cursor = MagicMock()
    return cursor

@pytest.fixture(scope="function")
def test_db():

    conn = pymysql.connect(
        **MYSQL_SETTINGS_ACTOR,
    )
    cursor = conn.cursor()

    db_name = APP_SETTINGS['db_name']

    cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
    cursor.execute(f"CREATE DATABASE {db_name}")
    cursor.execute(f"USE {db_name}")

    cursor.execute("""
        CREATE TABLE items (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(64),
            value INT
        )
    """)

    yield db_name

    cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
    conn.close()

def start_engine():
    stop_event = threading.Event()

    thread = threading.Thread(
        target=run,
        kwargs={"stop_event": stop_event},
        daemon=True,
    )

    thread.start()
    time.sleep(1)  # дать подняться бинлог-стримеру

    return stop_event, thread
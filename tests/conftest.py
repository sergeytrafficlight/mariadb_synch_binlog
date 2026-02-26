import pytest
import pymysql
import threading
import time
import clickhouse_connect
from unittest.mock import MagicMock
from config.config_test import MYSQL_SETTINGS, APP_SETTINGS, MYSQL_SETTINGS_ACTOR
from plugins_test.plugin_test import CLICKHOUSE_SETTINGS_ACTOR
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

def create_mariadb_db(mysql_settings=MYSQL_SETTINGS_ACTOR, app_settings=APP_SETTINGS):

    conn = pymysql.connect(
        **mysql_settings,
    )
    cursor = conn.cursor()

    db_name = app_settings['db_name']

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

    cursor.execute("""
            CREATE TABLE items2 (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(64),
                value INT
            )
        """)

    return db_name

def create_clickhouse_db(clickhouse_settings=CLICKHOUSE_SETTINGS_ACTOR):

    db_name = clickhouse_settings["database"]

    client = clickhouse_connect.get_client(
        host=clickhouse_settings["host"],
        port=clickhouse_settings["port"],
        username=clickhouse_settings["user"],
        password=clickhouse_settings["password"],
    )

    client.command(f"DROP DATABASE IF EXISTS {db_name}")
    client.command(f"CREATE DATABASE {db_name}")

    client.command(f"""
        CREATE TABLE {db_name}.items (
            id UInt64,
            name String,
            value Int32,
            version UInt64,
            deleted UInt8 DEFAULT 0,
            updated_at DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(version)
        ORDER BY id
        TTL updated_at + INTERVAL 7 DAY WHERE deleted = 1;
    """)


    client.command(f"""
        CREATE TABLE {db_name}.items2 (
            id UInt64,
            name String,
            value Int32,
            version UInt64,
            deleted UInt8 DEFAULT 0,
            updated_at DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(version)
        ORDER BY id
        TTL updated_at + INTERVAL 7 DAY WHERE deleted = 1;
    """)


    return db_name


def start_engine(MYSQL_SETTINGS, APP_SETTINGS):

    thread = threading.Thread(
        target=run,
        daemon=True,
        args=(MYSQL_SETTINGS, APP_SETTINGS,)
    )

    thread.start()
    time.sleep(1)  # дать подняться бинлог-стримеру

    return thread
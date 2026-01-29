import pytest
import pymysql
import time
from tests.conftest import start_engine
from config.config_test import MYSQL_SETTINGS_ACTOR, APP_SETTINGS

def generate_load(db_name):
    mysql_settings = MYSQL_SETTINGS_ACTOR
    mysql_settings['database'] = APP_SETTINGS['db_name']
    conn = pymysql.connect(
        **mysql_settings
    )
    cursor = conn.cursor()

    for i in range(10):
        print(f"generate insert")
        cursor.execute(
            "INSERT INTO items (name, value) VALUES (%s, %s)",
            (f"name{i}", i),
        )

    cursor.execute("UPDATE items SET value = value + 100")
    cursor.execute("DELETE FROM items WHERE id % 2 = 0")
    conn.commit()

    conn.close()

def test_engine_pipeline(test_db):

    db_name = test_db

    stop_event, engine_thread = start_engine()

    time.sleep(2)

    generate_load(db_name)

    # дать бинлогу доехать
    time.sleep(2)

    # стопаем engine
    stop_event.set()
    engine_thread.join(timeout=5)

    assert not engine_thread.is_alive()


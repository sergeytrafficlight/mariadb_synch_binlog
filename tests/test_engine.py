import pytest
import pymysql
import time
import os
import signal
from tests.conftest import start_engine
from config.config_test import MYSQL_SETTINGS_ACTOR, APP_SETTINGS
from plugins_test.plugin_test import statistic

def generate_load(db_name, count):
    mysql_settings = MYSQL_SETTINGS_ACTOR
    mysql_settings['database'] = APP_SETTINGS['db_name']
    conn = pymysql.connect(
        **mysql_settings
    )
    cursor = conn.cursor()

    for i in range(count):
        cursor.execute(
            "INSERT INTO items (name, value) VALUES (%s, %s)",
            (f"name{i}", i),
        )

    cursor.execute("UPDATE items SET value = value + 100")
    cursor.execute("DELETE FROM items WHERE id % 2 = 0")
    conn.commit()

    conn.close()

def test_engine_pipeline(test_db):

    load_count = 1000

    db_name = test_db

    engine_thread = start_engine()

    generate_load(db_name, load_count)

    time.sleep(2)

    os.kill(os.getpid(), signal.SIGINT)
    engine_thread.join(timeout=5)

    assert not engine_thread.is_alive()

    global statistic

    assert statistic.process_event_insert == load_count
    assert statistic.process_event_update == load_count
    assert statistic.process_event_delete == load_count / 2
    assert statistic.initiate_synch_mode == 1
    assert statistic.tear_down == 1
    assert statistic.initiate_full_regeneration == 1
    assert statistic.finished_full_regeneration == 1




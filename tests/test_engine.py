import pytest
import pymysql
import logging
import time
import os
import signal
from tests.conftest import start_engine, create_mariadb_db, create_clickhouse_db
from config.config_test import MYSQL_SETTINGS_ACTOR, APP_SETTINGS
from plugins_test.plugin_test import statistic

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s:%(lineno)d  - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


def generate_init_load(db_name, count):
    mysql_settings = MYSQL_SETTINGS_ACTOR
    mysql_settings['database'] = APP_SETTINGS['db_name']
    conn = pymysql.connect(
        **mysql_settings
    )
    cursor = conn.cursor()

    for i in range(count):
        name=f"name_{i}-init-load"
        logger.debug(f"insert init {name}")
        cursor.execute(
            "INSERT INTO items (name, value) VALUES (%s, %s)",
            (name, i),
        )

    conn.commit()

    cursor.execute("SHOW MASTER STATUS;")
    r = cursor.fetchall()
    logger.debug(f"binlog pos after insert {r}")

    conn.close()

def generate_load(db_name, count):
    mysql_settings = MYSQL_SETTINGS_ACTOR
    mysql_settings['database'] = APP_SETTINGS['db_name']
    conn = pymysql.connect(
        **mysql_settings
    )
    cursor = conn.cursor()

    for i in range(count):
        name=f"name_{i}-load"
        logger.debug(f"insert load {name}")
        cursor.execute(
            "INSERT INTO items (name, value) VALUES (%s, %s)",
            (name, i),
        )

    cursor.execute("UPDATE items SET value = value + 100")
    cursor.execute("DELETE FROM items WHERE id % 2 = 0")
    conn.commit()

    cursor.execute("SHOW MASTER STATUS;")
    r = cursor.fetchall()
    logger.debug(f"binlog pos after insert {r}")

    conn.close()

def _test_engine_pipeline():

    global statistic
    statistic.clear()

    binlog_file_path = APP_SETTINGS['binlog_file']
    try:
        os.remove(binlog_file_path)
    except FileNotFoundError:
        pass

    leads_count = 2

    db_name = create_mariadb_db()

    generate_init_load(db_name, leads_count)
    time.sleep(1)

    engine_thread = start_engine()

    generate_load(db_name, leads_count)

    time.sleep(2)

    os.kill(os.getpid(), signal.SIGINT)
    engine_thread.join(timeout=5)

    assert not engine_thread.is_alive()



    assert statistic.process_event_insert == leads_count * 2
    assert statistic.process_event_update == leads_count * 2
    assert statistic.process_event_delete == leads_count
    assert statistic.initiate_synch_mode == 1
    assert statistic.tear_down == 1
    assert statistic.initiate_full_regeneration == 1
    assert statistic.finished_full_regeneration == 1



def test_engine_pipeline_advanced():

    global statistic
    statistic.clear()

    binlog_file_path = APP_SETTINGS['binlog_file']
    try:
        os.remove(binlog_file_path)
    except FileNotFoundError:
        pass

    db_name = create_mariadb_db()
    db_clickhouse = create_clickhouse_db()

    binlog_file_path = APP_SETTINGS['binlog_file']
    try:
        os.remove(binlog_file_path)
    except FileNotFoundError:
        pass

    leads_count = 1

    generate_init_load(db_name, leads_count)

    time.sleep(1)

    engine_thread = start_engine()

    generate_load(db_name, leads_count)

    time.sleep(2)

    os.kill(os.getpid(), signal.SIGINT)
    engine_thread.join(timeout=5)
    assert not engine_thread.is_alive()


    generate_load(db_name, leads_count)

    engine_thread = start_engine()
    time.sleep(2)
    os.kill(os.getpid(), signal.SIGINT)
    engine_thread.join(timeout=5)
    assert not engine_thread.is_alive()

    assert statistic.process_event_insert == leads_count * 2
    assert statistic.process_event_update == leads_count * 2
    assert statistic.process_event_delete == leads_count
    assert statistic.initiate_synch_mode == 2
    assert statistic.tear_down == 2
    assert statistic.initiate_full_regeneration == 1
    assert statistic.finished_full_regeneration == 1



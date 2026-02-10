import pytest
import pymysql
import logging
import time
import os
import signal
import clickhouse_connect
from tests.conftest import start_engine, create_mariadb_db, create_clickhouse_db
from config.config_test import MYSQL_SETTINGS_ACTOR, APP_SETTINGS
from plugins_test.plugin_test import statistic, CLICKHOUSE_SETTINGS_ACTOR
from src.tools import get_health_answer

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
        #logger.debug(f"insert init {name}")
        cursor.execute(
            "INSERT INTO items (name, value) VALUES (%s, %s)",
            (name, i),
        )

    cursor.execute("UPDATE items SET value = value + 100")

    conn.commit()

    cursor.execute("SHOW MASTER STATUS;")
    r = cursor.fetchall()
    #logger.debug(f"binlog pos after insert {r}")

    conn.close()

def generate_load(db_name, count):
    mysql_settings = MYSQL_SETTINGS_ACTOR
    mysql_settings['database'] = APP_SETTINGS['db_name']
    conn = pymysql.connect(
        **mysql_settings
    )
    cursor = conn.cursor()

    cursor.execute("SHOW MASTER STATUS;")
    r = cursor.fetchall()
    #logger.debug(f"binlog before insert {r}")


    for i in range(count):
        name=f"name_{i}-load"
        #logger.debug(f"insert load {name}")
        cursor.execute(
            "INSERT INTO items (name, value) VALUES (%s, %s)",
            (name, i),
        )

    cursor.execute("UPDATE items SET value = value + 100")
    cursor.execute("DELETE FROM items WHERE id % 2 = 0")
    conn.commit()

    cursor.execute("SHOW MASTER STATUS;")
    r = cursor.fetchall()
    #logger.debug(f"binlog pos after insert {r}")

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

    client_ch = clickhouse_connect.get_client(
        host=CLICKHOUSE_SETTINGS_ACTOR["host"],
        port=CLICKHOUSE_SETTINGS_ACTOR["port"],
        username=CLICKHOUSE_SETTINGS_ACTOR["user"],
        password=CLICKHOUSE_SETTINGS_ACTOR["password"],
        database=CLICKHOUSE_SETTINGS_ACTOR["database"],
    )

    result = client_ch.query(
        '''
        SELECT
            COUNT(*) as count_rows,
            SUM(value) as sum_rows
        FROM
        (
            SELECT
                argMax(value, version) AS value,
                argMax(deleted, version) AS deleted
            FROM items
            GROUP BY id
        )       
        WHERE deleted = 0 
        '''
    )

    result = result.named_results()
    result = next(result)

    ch_count = int(result['count_rows'])
    ch_sum = int(result['sum_rows'])

    assert ch_count == 0
    assert ch_sum == 0

    client_ch.close()



    binlog_file_path = APP_SETTINGS['binlog_file']
    try:
        os.remove(binlog_file_path)
    except FileNotFoundError:
        pass

    leads_count = 2

    generate_init_load(db_name, leads_count)
    engine_thread = start_engine()
    generate_load(db_name, leads_count)

    time.sleep(2)

    os.kill(os.getpid(), signal.SIGINT)
    engine_thread.join(timeout=20)
    assert not engine_thread.is_alive()


    generate_load(db_name, leads_count)
    engine_thread = start_engine()
    time.sleep(2)
    logger.debug(f"stop thread")
    answer = get_health_answer(APP_SETTINGS['health_socket'])
    os.kill(os.getpid(), signal.SIGINT)
    engine_thread.join(timeout=10)
    logger.debug("join done")

    print(f"answer: {answer}")
    assert not engine_thread.is_alive()

    assert statistic.process_event_insert == leads_count * 3
    assert statistic.initiate_synch_mode == 2
    assert statistic.tear_down == 2
    assert statistic.initiate_full_regeneration == 1
    assert statistic.finished_full_regeneration == 1


    conn_mariadb = pymysql.connect(
        **MYSQL_SETTINGS_ACTOR,
    )
    cursor_mariadb = conn_mariadb.cursor()

    client_ch = clickhouse_connect.get_client(
        host=CLICKHOUSE_SETTINGS_ACTOR["host"],
        port=CLICKHOUSE_SETTINGS_ACTOR["port"],
        username=CLICKHOUSE_SETTINGS_ACTOR["user"],
        password=CLICKHOUSE_SETTINGS_ACTOR["password"],
        database=CLICKHOUSE_SETTINGS_ACTOR["database"],
    )

    cursor_mariadb.execute("SELECT COUNT(*), SUM(value) FROM items;")
    row_mariadb = cursor_mariadb.fetchall()
    mariadb_count = int(row_mariadb[0][0])
    mariadb_sum = int(row_mariadb[0][1])

    result = client_ch.query(
        '''
        SELECT
            COUNT(*) as count_rows,
            SUM(value) as sum_rows
        FROM
        (
            SELECT
                argMax(value, version) AS value,
                argMax(deleted, version) AS deleted
            FROM items
            GROUP BY id
        )       
        WHERE deleted = 0 
        '''
    )

    result = result.named_results()
    result = next(result)

    ch_count = int(result['count_rows'])
    ch_sum = int(result['sum_rows'])

    assert mariadb_count == ch_count
    assert mariadb_sum == ch_sum




def _test_health_server():

    global statistic
    statistic.clear()
    binlog_file_path = APP_SETTINGS['binlog_file']
    try:
        os.remove(binlog_file_path)
    except FileNotFoundError:
        pass

    db_name = create_mariadb_db()
    db_clickhouse = create_clickhouse_db()

    leads_count = 100000
    generate_init_load(db_name, leads_count)
    engine_thread = start_engine()
    time.sleep(2)
    answer = get_health_answer(APP_SETTINGS['health_socket'])
    print(answer)
import sys
import threading

import pytest
import pymysql
import logging
import time
import os
import signal
import clickhouse_connect

from .conftest import start_engine, create_mariadb_db, create_clickhouse_db


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


def generate_init_load(count):
    from config.config_test import MYSQL_SETTINGS_ACTOR, APP_SETTINGS, MYSQL_SETTINGS
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
            (name, i+1),
        )

        cursor.execute(
            "INSERT INTO items2 (name, value) VALUES (%s, %s)",
            (name, i+1),
        )

    cursor.execute("UPDATE items SET value = value + 100")

    conn.commit()

    cursor.execute("SHOW MASTER STATUS;")
    r = cursor.fetchall()
    #logger.debug(f"binlog pos after insert {r}")

    conn.close()

def generate_load(count, only_insert=False):
    from config.config_test import MYSQL_SETTINGS_ACTOR, APP_SETTINGS, MYSQL_SETTINGS
    mysql_settings = MYSQL_SETTINGS_ACTOR
    mysql_settings['database'] = APP_SETTINGS['db_name']
    conn = pymysql.connect(
        **mysql_settings
    )
    cursor = conn.cursor()

    for i in range(count):
        name=f"name_{i}-load"
        #logger.debug(f"insert load {name}")
        cursor.execute(
            "INSERT INTO items (name, value) VALUES (%s, %s)",
            (name, i+1),
        )
        cursor.execute(
            "INSERT INTO items2 (name, value) VALUES (%s, %s)",
            (name, i+1),
        )

    if not only_insert:
        cursor.execute("UPDATE items SET value = value + 100")
        cursor.execute("DELETE FROM items WHERE id % 2 = 0")
    conn.commit()
    conn.close()

def generate_fake_xid(count):
    from config.config_test import MYSQL_SETTINGS_ACTOR, APP_SETTINGS, MYSQL_SETTINGS
    mysql_settings = MYSQL_SETTINGS_ACTOR
    mysql_settings['database'] = APP_SETTINGS['db_name']
    conn = pymysql.connect(
        **mysql_settings
    )
    cursor = conn.cursor()

    for i in range(count):
        name=f"name_{i}-load"
        #logger.debug(f"insert load {name}")
        cursor.execute(
            "INSERT INTO items_fake_xid (name, value) VALUES (%s, %s)",
            (name, i+1),
        )
        conn.commit()

    conn.close()

def change_binlog_file():
    from config.config_test import MYSQL_SETTINGS_ACTOR, APP_SETTINGS, MYSQL_SETTINGS
    mysql_settings = MYSQL_SETTINGS_ACTOR
    mysql_settings['database'] = APP_SETTINGS['db_name']
    conn = pymysql.connect(
        **mysql_settings
    )
    cursor = conn.cursor()

    try:
        cursor.execute(
            "FLUSH BINARY LOGS;"
        )
    except Exception as e:
        logger.critical(f"Flush binary logs error: {str(e)}")
        sys.exit(-1)

    conn.close()

def get_binary_logs():

    from config.config_test import MYSQL_SETTINGS_ACTOR, APP_SETTINGS, MYSQL_SETTINGS
    from src.tools import binlog_file
    mysql_settings = MYSQL_SETTINGS_ACTOR
    mysql_settings['database'] = APP_SETTINGS['db_name']
    conn = pymysql.connect(
        **mysql_settings
    )
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SHOW BINARY LOGS;"
        )

        result = cursor.fetchall()
        binlogs = []
        for r in result:
            binlogs.append(binlog_file(file_path='/var/tmp/1', file=r[0], pos=r[1]))
        conn.close()
        return binlogs



    except Exception as e:
        logger.critical(str(e))
        sys.exit(-1)




def compare_db(equal = True):
    from config.config_test import MYSQL_SETTINGS_ACTOR, APP_SETTINGS, MYSQL_SETTINGS
    from plugins_test.plugin_test import statistic, CLICKHOUSE_SETTINGS_ACTOR
    mysql_settings = MYSQL_SETTINGS_ACTOR
    mysql_settings['database'] = APP_SETTINGS['db_name']

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
    mariadb_count = int(row_mariadb[0][0] or 0)
    mariadb_sum = int(row_mariadb[0][1] or 0)

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

    if equal:
        assert mariadb_count == ch_count
        assert mariadb_sum == ch_sum
    else:
        assert mariadb_count != ch_count
        assert mariadb_sum != ch_sum

    conn_mariadb.close()
    client_ch.close()

def clear_binlog_file():
    from config.config_test import MYSQL_SETTINGS_ACTOR, APP_SETTINGS, MYSQL_SETTINGS
    binlog_file_path = APP_SETTINGS['binlog_file']
    try:
        os.remove(binlog_file_path)
    except FileNotFoundError:
        pass

def _start():
    from config.config_test import MYSQL_SETTINGS_ACTOR, APP_SETTINGS, MYSQL_SETTINGS
    from src.tools import get_health_answer, get_binlog_diff, start, stop, binlog_file
    r = start(MYSQL_SETTINGS, APP_SETTINGS, as_thread=True)
    time.sleep(1)
    return r

def _stop(consumer):
    from src.tools import get_health_answer, get_binlog_diff, start, stop, binlog_file
    stop(consumer)
    assert not consumer.is_alive()

def _wait_lag():
    from config.config_test import MYSQL_SETTINGS_ACTOR, APP_SETTINGS, MYSQL_SETTINGS
    from src.tools import get_health_answer
    while True:
        answer = get_health_answer(APP_SETTINGS['health_socket'])
        lag = answer['binlog_diff']

        if lag is None:
            time.sleep(1)
            continue
        if not lag:
            break


def test_start_stop():

    db_name = create_mariadb_db()
    db_clickhouse = create_clickhouse_db()

    clear_binlog_file()
    consumer = _start()
    _wait_lag()
    _stop(consumer)


def test_init_load_insert():

    from plugins_test.plugin_test import statistic
    from plugins_test.plugin_test import set_emulate_error

    statistic.clear()

    db_name = create_mariadb_db()
    db_clickhouse = create_clickhouse_db()

    clear_binlog_file()

    compare_db()

    init_load_count = 2
    generate_init_load(init_load_count)
    consumer = _start()
    _wait_lag()

    assert statistic.initiate_full_regeneration == 1
    assert statistic.process_event_insert == init_load_count

    _stop(consumer)

    compare_db()


    statistic.clear()

    consumer = _start()
    assert statistic.process_event_insert == 0

    generate_load(1, True)

    _wait_lag()

    assert statistic.process_event_insert == 1

    _stop(consumer)

    compare_db()

    statistic.clear()

    consumer = _start()
    assert statistic.process_event_insert == 0

    #generate error
    set_emulate_error(True)

    generate_load(1, True)

    _stop(consumer)
    compare_db(equal=False)

    set_emulate_error(False)

    consumer = _start()
    _wait_lag()
    _stop(consumer)

    compare_db()



def test_engine_stresstest():
    from config.config_test import MYSQL_SETTINGS_ACTOR, APP_SETTINGS, MYSQL_SETTINGS
    from src.tools import get_health_answer, get_binlog_diff, start, stop, binlog_file
    from plugins_test.plugin_test import statistic
    from plugins_test.plugin_test import set_emulate_error

#    global statistic
    statistic.clear()

    clear_binlog_file()

    db_name = create_mariadb_db()
    db_clickhouse = create_clickhouse_db()

    consumer = _start()

    time.sleep(1)
    assert statistic.initiate_full_regeneration == 1

    compare_db()

    set_emulate_error(False)

    stress_test_duration_s = 20

    def _stress_load_thread_fake_xid(duration_s):
        start_time = time.time()

        while time.time() - start_time < duration_s:
            # print(f"generate load")
            generate_fake_xid(20)
            time.sleep(0.13)




    def _stress_load_thread(duration_s):
        start_time = time.time()

        while time.time() - start_time < duration_s:
            #print(f"generate load")
            generate_load(10, True)
            time.sleep(0.13)


    def _change_binlog_file(duration_s):

        duration_s = duration_s / 4
        for i in range(3):
            time.sleep(duration_s)
            change_binlog_file()


    t_list = []



    for i in range(5):
        t = threading.Thread(target=_stress_load_thread_fake_xid, args=(stress_test_duration_s, ))
        t.start()
        t_list.append(t)



    for i in range(20):
        t = threading.Thread(target=_stress_load_thread, args=(stress_test_duration_s, ))
        t.start()
        t_list.append(t)

    t = threading.Thread(target=_change_binlog_file, args=(stress_test_duration_s, ))
    t.start()
    t_list.append(t)


    start_time = time.time()

    _stop(consumer)

    print(f"Generating errors")
    while time.time() - start_time < stress_test_duration_s:
        statistic.clear()
        consumer = _start()
        set_emulate_error(True)
        time.sleep(1)
        _stop(consumer)
        set_emulate_error(False)

        assert statistic.initiate_full_regeneration == 0

        print(f'Emulate error, timer: {(time.time() - start_time)} of {stress_test_duration_s}')



    print(f"Waiting for threads")
    for t in t_list:
        t.join()


    #waiting for gtid lag == 0
    set_emulate_error(False)
    consumer = _start()

    print(f"Waiting for lag == 0")
    while True:
        time.sleep(1)
        answer = get_health_answer(APP_SETTINGS['health_socket'])
        lag = answer['binlog_diff']
        print(f"binlog lag: {lag}")
        if lag is None:
            continue
        if not lag:
            break

    _stop(consumer)
    compare_db()




def test_gtid_diff():
    from src.tools import get_health_answer, get_binlog_diff, start, stop, binlog_file

    def _check_binlog_in_range(a):
        binary_logs = get_binary_logs()
        for log in binary_logs:
            if a.file != log.file:
                continue
            if a.pos <= log.pos and a.pos >= 0:
                return True
            return False
        return False

    def _calc_diff(binary_logs, a, b):
        assert _check_binlog_in_range(a), f"Binlog A out of range: {a}"
        assert _check_binlog_in_range(b), f"Binlog A out of range: {b}"
        assert b >= a, f"B {b} is less than A {a}"

        diff = 0
        found_a = False
        found_b = False
        for log in binary_logs:
            if log.file < a.file:
                continue
            if log.file == a.file:
                found_a = True
                if a.file == b.file:
                    found_b = True
                    diff += b.pos - a.pos
                    break

                diff += log.pos - a.pos
                continue

            if log.file < b.file:
                diff += log.pos

                continue
            if log.file == b.file:
                found_b = True
                diff += b.pos

                break
            raise ValueError(f"Current log ({log}) > b ({b})")

        assert found_a and found_b, f"Not all logs found A: {found_a} B: {found_b}"
        return diff




    binary_logs = get_binary_logs()

    assert len(binary_logs) > 3, f"Binary logs len have to me greater then 3, for this test. Try to run 'FLUSH BINARY LOGS;' some times, to reach this ;)"

    first_binary_log = binary_logs[0].copy()
    last_binary_log = binary_logs[0].copy()
    diff = _calc_diff(binary_logs, first_binary_log, last_binary_log)
    assert diff == 0

    last_binary_log.pos += 1

    assert not _check_binlog_in_range(last_binary_log)
    last_binary_log.pos -= 1
    assert _check_binlog_in_range(last_binary_log)

    first_binary_log.pos -= 1
    assert _check_binlog_in_range(first_binary_log) #may be pos is < 0 ?

    diff = _calc_diff(binary_logs, first_binary_log, last_binary_log)
    assert diff == 1

    diff = _calc_diff(binary_logs, first_binary_log, last_binary_log)
    assert diff == 1

    first_binary_log.pos = 0
    last_binary_log = binary_logs[-1].copy()
    diff = _calc_diff(binary_logs, first_binary_log, last_binary_log)
    check_diff = 0
    for log in binary_logs:
        check_diff += log.pos

    assert diff == check_diff


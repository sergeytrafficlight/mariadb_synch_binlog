import os
import re
import json
import socket
import time
import pymysql
import importlib
import threading
import clickhouse_connect
from functools import total_ordering

@total_ordering
class binlog_file:

    def __init__(self, file_path: str, file: str =None, pos: int =None):
        self.file = file
        self.pos = int(pos) if pos else 0
        self.file_path = file_path

    def __str__(self):
        return f"file: {self.file} pos: {self.pos} [{self.file_path}]"

    def __eq__(self, other):
        if not isinstance(other, binlog_file):
            return NotImplemented
        return self.file == other.file and self.pos == other.pos

    def __lt__(self, other):
        if not isinstance(other, binlog_file):
            return NotImplemented
        if self.file == other.file:
            return self.pos < other.pos
        return self.file < other.file

    def copy(self):
        return binlog_file(file_path=self.file_path, file=self.file, pos=self.pos)

    def load(self):
        if not os.path.exists(self.file_path):
            return False
        try:
            with open(self.file_path, "r") as f:
                data = json.load(f)
                self.file = data.get("log_file")
                self.pos = data.get("log_pos")
            # проверка, что данные валидные
            if not isinstance(self.file, str) or not isinstance(self.pos, int):
                return False
            return True
        except (json.JSONDecodeError, IOError, ValueError):
            return False

    def save(self):
        """Сохраняет текущий offset в JSON (атомарно через tmp-файл)."""
        tmp_file = self.file_path + ".tmp"
        data = {
            "log_file": self.file,
            "log_pos": self.pos
        }
        try:
            with open(tmp_file, "w") as f:
                json.dump(data, f)
            # атомарная замена
            os.replace(tmp_file, self.file_path)
        except IOError as e:
            print(f"Ошибка сохранения binlog offset: {e}")
            return False
        return True



class plugin_wrapper:

    def __init__(self, module_path):

        module = importlib.import_module(module_path)

        #инициалзация общего модуля, вызывается 1 раз при старте
        self.init = getattr(module, 'init')
        # инициалзация процесса полной регенерации данных, вызывает я 1 раз, перед началом выборки данных из БД
        self.initiate_full_regeneration = getattr(module, 'initiate_full_regeneration')
        # завершение процесса регенерации
        self.finished_full_regeneration = getattr(module, 'finished_full_regeneration')
        # инициалзация процесса чтения binlog-а и синхронизации в real-time, вызывается 1 раз
        self.initiate_synch_mode = getattr(module, 'initiate_synch_mode')
        # вызывается перед запуском воркеров, которые должна обработать через process_event, все считанные данные,
        # вызывается всегда в одном потоке, и гарантированно изолированно от process_event
        self.initiate_dropdown_workers = getattr(module, 'initiate_dropdown_workers')
        # завершение работы модуля
        self.tear_down = getattr(module, 'tear_down')
        # вызывается в мультипоточном режиме, для обработки накопленных данных
        self.process_event = getattr(module, 'process_event')
        # вызывается после завершения работы всех воркеров, в рамках собранного пакета данных, для сброса данных в хранилище
        self.dump_values = getattr(module, 'dump_values')


class regeneration_threads_controller:

    class table_info:

        def __init__(self):
            self.current_id = None

            self.rows_count = 0
            self.rows_parsed = 0

            self.max_id = 0


    def __init__(self, threads_count):
        self.tables = {}
        self.lock = threading.Lock()
        self.barrier = threading.Barrier(threads_count)
        self.start_at = None
        self.rows_parsed = 0

    def get_and_update_id(self, table, count):
        result = None
        with self.lock:
            if table not in self.tables:
                self.tables[table] = self.table_info()

            result = self.tables[table].current_id or 0
            self.tables[table].current_id += count

        return result

    def add_parsed_count(self, count):
        with self.lock:
            self.rows_parsed += count

    def is_end(self, table):
        with self.lock:
            return self.tables[table].current_id >= self.tables[table].max_id

    def put_rows_count(self, table, count, min_id, max_id):
        if self.start_at is None:
            self.start_at = time.time()

        if min_id is None:
            min_id = 0
        if max_id is None:
            max_id = 0
        with self.lock:
            if table not in self.tables:
                self.tables[table] = self.table_info()
            if count > self.tables[table].rows_count:
                self.tables[table].rows_count = count
            if self.tables[table].current_id is None or min_id < self.tables[table].current_id:
                self.tables[table].current_id = min_id
            if max_id > self.tables[table].max_id:
                self.tables[table].max_id = max_id

    def statistic(self):
        with self.lock:
            total = 0

            for k, v in self.tables.items():
                total += v.rows_count

            if self.start_at and self.rows_parsed < total and self.rows_parsed:
                time_diff = time.time() - self.start_at
                time_per_one = float(time_diff) / self.rows_parsed
                estimate = (total - self.rows_parsed) * time_per_one
            else:
                estimate = None

            return (total, self.rows_parsed, estimate)


    def is_total_parsed(self):
        total, parsed, _ = self.statistic()
        return total == parsed



class insert_item_row:

    def __init__(self, table_name: str, keys: [], values: []):
        self.table_name = table_name
        self.keys = keys
        self.values = values


class insert_buffer:

    def __init__(self, triggering_rows_count = 1_000):

        self.lock = threading.Lock()
        self.items = []

    def push(self, table, columns, data) -> bool:
        with self.lock:
            self.items.append(insert_item_row(table, columns, data))

    def get_similar_pack_clear(self):

        with self.lock:
            if not len(self.items):
                return None

            pack = []

            for i, item in enumerate(self.items):
                if len(pack):
                    if pack[0].table_name != item.table_name:
                        break
                    if pack[0].keys != item.keys:
                        raise Exception(
                            f"All rows for table '{item.table_name}' must have identical keys "
                            f"and the same key order.\n"
                            f"Row[0] keys: {pack[0].keys}\n"
                            f"Row[1] keys: {item.keys}"
                        )
                pack.append(item)

            del self.items[:len(pack)]
            return pack


def get_health_answer(socket_path):

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(socket_path)
        data = s.recv(4096)

    return json.loads(data.decode())
    #print(json.loads(data))

def get_binlogs(mysql_settings):
    conn = pymysql.connect(
        **mysql_settings
    )
    cursor = conn.cursor()

    cursor.execute(
        "SHOW BINARY LOGS;"
    )

    result = cursor.fetchall()
    binlogs = []

    for r in result:
        binlogs.append(binlog_file(file_path='/var/tmp/1', file=r[0], pos=r[1]))

    conn.close()

    return binlogs


def check_binlog_in_range(mysql_settings, binlog, binlogs=None):
    if binlogs is None:
        binlogs = get_binlogs(mysql_settings)
    for log in binlogs:
        if binlog.file != log.file:
            continue
        if binlog.pos >= 0 and binlog.pos <= log.pos:
            return True
        return False
    return False

def get_binlog_diff(mysql_settings, binlog_a, binlog_b):

    if binlog_a is None or binlog_b is None:
        return None

    binlogs = get_binlogs(mysql_settings)
    if not check_binlog_in_range(mysql_settings, binlog_a, binlogs=binlogs):
        raise ValueError(f"Binlog {binlog_a} out of range")
    if not check_binlog_in_range(mysql_settings, binlog_b, binlogs=binlogs):
        raise ValueError(f"Binlog {binlog_b} out of range")

    diff = 0
    for log in binlogs:
        if log.file < binlog_a.file:
            continue

        if log.file == binlog_a.file:
            if binlog_a.file == binlog_b.file:
                diff += binlog_b.pos - binlog_a.pos
                break
            else:
                diff += log.pos - binlog_a.pos
                continue

        if log.file < binlog_b.file:
            diff += log.pos
            continue

        if log.file == binlog_b.file:
            diff += binlog_b.pos
            break

        raise ValueError(f"Current log ({log}) > b ({binlog_b})")

    return diff



def start(MYSQL_SETTINGS, APP_SETTINGS, as_thread=True):

    from src.engine import run

    if as_thread:

        thread = threading.Thread(
            target=run,
            daemon=True,
            args=(MYSQL_SETTINGS, APP_SETTINGS,)
        )

        thread.start()
        time.sleep(1)  # дать подняться бинлог-стримеру

        return thread
    else:

        return run(MYSQL_SETTINGS, APP_SETTINGS)

def stop(thread, wait_interval_s = 20):
    from src.engine import stop
    stop()

    thread.join(wait_interval_s)

def get_binlog_from_db(MYSQL_SETTINGS, APP_SETTINGS):
    conn = pymysql.connect(**MYSQL_SETTINGS)
    cursor = conn.cursor()
    cursor.execute("SHOW MASTER STATUS;")
    r = cursor.fetchall()

    binlog = binlog_file(file_path=APP_SETTINGS['binlog_file'], file=r[0][0], pos=r[0][1])

    conn.close()
    return binlog

def preflight_check(MYSQL_SETTINGS, APP_SETTINGS):
    from .engine import preflight_check_ex
    conn = pymysql.connect(**MYSQL_SETTINGS)
    cursor = conn.cursor()
    preflight_check_ex(cursor, MYSQL_SETTINGS, APP_SETTINGS)
    conn.close()

class process_event_result:

    def __init__(self, table_name, columns, values):
        self.table_name = table_name
        self.columns = columns
        self.values = values
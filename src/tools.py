import os
import re
import json
import socket
import time
import pymysql
import importlib
import threading
import clickhouse_connect


class binlog_file:

    def __init__(self, file_path, file=None, pos=None):
        self.file = file
        self.pos = pos
        self.file_path = file_path

    def __str__(self):
        return f"file: {self.file} pos: {self.pos}"

    def __eq__(self, other):
        return self.file == other.file and self.pos == other.pos

    def __ne__(self, other):
        return not self == other

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

        self.init = getattr(module, 'init')
        self.initiate_full_regeneration = getattr(module, 'initiate_full_regeneration')
        self.finished_full_regeneration = getattr(module, 'finished_full_regeneration')
        self.initiate_synch_mode = getattr(module, 'initiate_synch_mode')
        self.tear_down = getattr(module, 'tear_down')
        self.process_event = getattr(module, 'process_event')
        self.XidEvent = getattr(module, 'XidEvent')


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

    def get_and_update_id(self, table, count):
        result = None
        with self.lock:
            if table not in self.tables:
                self.tables[table] = self.table_info()

            result = self.tables[table].current_id or 0
            self.tables[table].current_id += count

        return result

    def add_parsed_count(self, table, count):
        with self.lock:
            self.tables[table].rows_parsed += count

    def is_end(self, table):
        with self.lock:
            return self.tables[table].current_id >= self.tables[table].max_id

    def put_rows_count(self, table, count, min_id, max_id):
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
            parsed = 0
            for k, v in self.tables.items():
                total += v.rows_count
                parsed += v.rows_parsed
            return (total, parsed)



class insert_item:

    def __init__(self, table_name: str, keys: [], values: [], binlog):
        self.table_name = table_name
        self.keys = keys
        self.values = values
        self.binlog = binlog


class insert_buffer:

    def __init__(self, triggering_rows_count = 1_000):

        self.lock = threading.Lock()
        self.triggering_rows_count = triggering_rows_count
        self.items = []

    def push(self, table, columns, data, binlog) -> bool:
        #return triggered

        with self.lock:
            self.items.append(insert_item(table, columns, data, binlog))
            return len(self.items) > self.triggering_rows_count

    def overload(self):
        with self.lock:
            return len(self.items) > self.triggering_rows_count

    def len(self):
        with self.lock:
            return len(self.items)

    def get_similar_pack_clear(self):

        with self.lock:
            if not len(self.items):
                return []

            pack = []

            for i, item in enumerate(self.items):
                if len(pack):
                    if pack[0].table_name != item.table_name or pack[0].keys != item.keys:
                        break
                pack.append(item)


            del self.items[:len(pack)]
            return pack

def get_health_answer(socket_path):

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(socket_path)
        data = s.recv(4096)

    return json.loads(data.decode())
    #print(json.loads(data))

def get_binlog_diff(binlog_a, binlog_b):

    if binlog_a is None or binlog_b is None:
        return None

    if binlog_a.file != binlog_b.file:
        return None


    return binlog_b.pos - binlog_a.pos

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

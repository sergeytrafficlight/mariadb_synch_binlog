import os
import re
import json
import socket

import importlib
import threading
import clickhouse_connect


class binlog_file:

    def __init__(self, file_path):
        self.file = None
        self.pos = None
        self.file_path = file_path

    def __str__(self):
        return f"file: {self.file} pos: {self.pos}"

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

    def __init__(self, table_name: str, keys: [], values: []):
        self.table_name = table_name
        self.keys = keys
        self.values = values


class insert_buffer:

    def __init__(self, triggering_rows_count = 1_000):

        self.lock = threading.Lock()
        self.triggering_rows_count = triggering_rows_count
        self.items = []

    def push(self, table, columns, data) -> bool:
        #return triggered

        with self.lock:
            self.items.append(insert_item(table, columns, data))
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
                if i == 0:
                    pack.append(item)
                    continue
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

def get_gtid_diff(gtid_a, gtid_b):

    def parse_gtid(gtid):
        r = {}
        for g in gtid.split(','):
            g = g.strip()
            if not g:
                continue

            d, _, s = g.split('-')
            d = int(d)
            s = int(s)

            r[d] = max(s, r.get(d, 0))
        return r

    if gtid_a is None or gtid_b is None:
        return 0

    a = parse_gtid(gtid_a)
    b = parse_gtid(gtid_b)

    result = 0

    for domain, seq_b in b.items():
        if domain not in a:
            continue
        seq_a = a[domain]
        if seq_b > seq_a:
            result += seq_b - seq_a

    return result

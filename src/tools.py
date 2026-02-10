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

    def __init__(self):
        self.current_id = 0
        self.lock = threading.Lock()

    def get_and_update_id(self, count):
        result = None
        with self.lock:
            result = self.current_id
            self.current_id += count
        return result


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

    return data.decode()
    #print(json.loads(data))

def get_gtid_diff(gtid_a, gtid_b):
    pass
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

class clickhouse_connection_pool:

    def __init__(self, credentials):
        self.lock = threading.Lock()
        self.credentials = credentials
        self.connectors = {}

    def __del__(self):
        with self.lock:
            for k in list(self.connectors):
                self.connectors[k].close()
                del self.connectors[k]

    def get_connector(self):
        thread_id = threading.get_ident()
        with self.lock:
            if thread_id in self.connectors:
                return self.connectors[thread_id]
            client = clickhouse_connect.get_client(
                host=self.credentials["host"],
                port=self.credentials["port"],
                username=self.credentials["user"],
                password=self.credentials["password"],
                database=self.credentials["database"],
            )

            return client

class insert_buffer:

    def __init__(self):

        self.lock = threading.Lock()
        self.tables_data = {}
        self.tables_columns = {}

    def push(self, table, columns, data) -> int:
        with self.lock:
            if table not in self.tables_data:
                self.tables_data[table] = []
                self.tables_columns[table] = columns
            else:
                assert self.tables_columns[table] == columns
                
            self.tables_data[table].append(data)
            return len(self.tables_data[table])

    def get_and_clear(self, table):
        with self.lock:
            r = self.tables[table]
            self.tables[table] = []
            return r

    def get_tables(self):
        with self.lock:
            return self.tables.keys()





def get_health_answer(socket_path):

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(socket_path)
        data = s.recv(4096)

    return data.decode()
    #print(json.loads(data))
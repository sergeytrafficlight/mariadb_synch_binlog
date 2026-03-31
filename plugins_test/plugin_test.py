import clickhouse_connect
import threading
import time
import logging

logger = logging.getLogger(__name__)
#logger.setLevel(logging.DEBUG)

CLICKHOUSE_SETTINGS_ACTOR = {
    "host": "127.0.0.1",
    "port": 8123,  # HTTP
    "user": "binlog_actor",
    "password": "strong_pass",
    "database": "mariadb_synch_binlog_tmp_test",
}

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s:%(lineno)d  - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

#for pytest

class statistic_class:

    def __init__(self):
        self.lock = threading.Lock()
        self.init = 0
        self.initiate_full_regeneration = 0
        self.finished_full_regeneration = 0
        self.initiate_synch_mode = 0
        self.tear_down = 0
        self.process_event_insert = 0
        self.process_event_update = 0
        self.process_event_delete = 0


    def clear(self):
        with self.lock:
            self.init = 0
            self.initiate_full_regeneration = 0
            self.finished_full_regeneration = 0
            self.initiate_synch_mode = 0
            self.tear_down = 0
            self.process_event_insert = 0
            self.process_event_update = 0
            self.process_event_delete = 0


statistic = statistic_class()
version = None
emulate_error = False

def set_emulate_error(v):
    global emulate_error
    emulate_error = v

def dump_values(table_name, columns, values):
    global emulate_error
    logger.debug("push to clickhouse")
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_SETTINGS_ACTOR["host"],
        port=CLICKHOUSE_SETTINGS_ACTOR["port"],
        username=CLICKHOUSE_SETTINGS_ACTOR["user"],
        password=CLICKHOUSE_SETTINGS_ACTOR["password"],
        database=CLICKHOUSE_SETTINGS_ACTOR["database"],
    )


    if not emulate_error:

        client.insert(
            table=table_name,
            column_names=columns,
            data=values
        )

    else:
        client.close()
        logger.debug("Emulate error")
        raise Exception(f"Emulate error")
    client.close()



def init():
    from src.tools import insert_buffer
    from src.synch_storage import version_lock
    global statistic, version

    logger.debug("INIT")
    statistic.init += 1
    version = version_lock()




def initiate_full_regeneration():
    #print(f'initiate_full_regeneration')
    global statistic
    statistic.initiate_full_regeneration +=1

def finished_full_regeneration():

    global statistic, insert_storage
    statistic.finished_full_regeneration +=1


def initiate_synch_mode():
    #print('initiate_synch_mode')
    global statistic, version
    statistic.initiate_synch_mode +=1

    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_SETTINGS_ACTOR["host"],
        port=CLICKHOUSE_SETTINGS_ACTOR["port"],
        username=CLICKHOUSE_SETTINGS_ACTOR["user"],
        password=CLICKHOUSE_SETTINGS_ACTOR["password"],
        database=CLICKHOUSE_SETTINGS_ACTOR["database"],
    )

    result = client.query(
        "SELECT max(version) FROM items"
    )

    version_id = result.result_rows[0][0]
    if not version_id:
        #none
        version.set_version(1)
    else:
        version.set_version(version_id)

    client.close()

def tear_down():
    #print('tear_down')
    global statistic
    statistic.tear_down +=1

    logger.debug("DEINIT")


def process_event(event_type, table, event):
    from src.tools import process_event_result
    global version
    if table != 'items':
        return []


    #ch_connector = clickhouse_connectors.get_connector()

    if event_type == 'insert':
        logger.debug(f"insert event {table} event: {event}")

        with statistic.lock:
            statistic.process_event_insert +=1

        event['version'] = version.get_version()

        columns = list(event.keys())
        values = [event[col] for col in columns]

        return [process_event_result(table_name=table, columns=columns, values=values)]

    elif event_type == 'update':
        with statistic.lock:
            statistic.process_event_update += 1
        logger.debug(f"update event {table} event: {event}")

        after = event['after_values']
        after['version'] = version.get_version()

        columns = list(after.keys())
        values = [ after[col] for col in columns ]

        return [process_event_result(table_name=table, columns=columns, values=values)]

    elif event_type == 'delete':
        with statistic.lock:
            statistic.process_event_delete += 1
        logger.debug(f"delete event {table} event: {event}")
        deleted_record = event['values']
        deleted_record['deleted'] = 1
        deleted_record['version'] = version.get_version()

        columns = list(deleted_record.keys())
        values = [deleted_record[col] for col in columns]

        return [process_event_result(table_name=table, columns=columns, values=values)]

    else:
        raise RuntimeError(f"Unknown event type '{event_type}'")



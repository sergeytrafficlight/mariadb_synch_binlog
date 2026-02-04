import clickhouse_connect
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

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
        self.init = 0
        self.initiate_full_regeneration = 0
        self.finished_full_regeneration = 0
        self.initiate_synch_mode = 0
        self.tear_down = 0
        self.process_event_insert = 0
        self.process_event_update = 0
        self.process_event_delete = 0

    def clear(self):
        self.init = 0
        self.initiate_full_regeneration = 0
        self.finished_full_regeneration = 0
        self.initiate_synch_mode = 0
        self.tear_down = 0
        self.process_event_insert = 0
        self.process_event_update = 0
        self.process_event_delete = 0


statistic = statistic_class()
version_id = 1
ch_connector = None

def init():
    global ch_connector, statistic
    statistic.init += 1

    ch_connector = clickhouse_connect.get_client(
        host=CLICKHOUSE_SETTINGS_ACTOR["host"],
        port=CLICKHOUSE_SETTINGS_ACTOR["port"],
        username=CLICKHOUSE_SETTINGS_ACTOR["user"],
        password=CLICKHOUSE_SETTINGS_ACTOR["password"],
        database=CLICKHOUSE_SETTINGS_ACTOR["database"],
    )


def initiate_full_regeneration():
    #print(f'initiate_full_regeneration')
    global statistic
    statistic.initiate_full_regeneration +=1

def finished_full_regeneration():
    #print('initiate_full_regeneration')
    global statistic
    statistic.finished_full_regeneration +=1

def initiate_synch_mode():
    #print('initiate_synch_mode')
    global statistic, version_id
    statistic.initiate_synch_mode +=1

    result = ch_connector.query(
        "SELECT max(version) FROM items"
    )

    version_id = result.result_rows[0][0]
    if not version_id:
        #none
        version_id = 1



def tear_down():
    #print('tear_down')
    global statistic
    statistic.tear_down +=1


def process_event(event_type, schema, table, event):
    # In full regeneration mode, this code may be executed in multiple threads.
    # In sync mode, it is executed in a single-threaded context.
    # print(f"Event type: {event_type}, schema: {schema}, table: {table}, event: {event}")

    global statistic, ch_connector, version_id

    if event_type == 'insert':
        logger.debug(f"insert event {schema}.{table} event: {event}")
        statistic.process_event_insert +=1

        version_id += 1

        event['version'] = version_id

        columns = list(event.keys())
        values = [event[col] for col in columns]

        ch_connector.insert(
            table=table,
            data=[values],
            column_names=columns
        )

        # I intentionally ignore multithreading here.
        # During the full regeneration phase, multithreading does not make much sense for this logic.
        # In sync mode, the code runs in a single thread, and it is important that version_id
        # is incremented correctly and deterministically.
        # This approach is not suitable for production use and should be treated as a temporary solution.

    elif event_type == 'update':
        statistic.process_event_update += 1
        logger.debug(f"update event {schema}.{table} event: {event}")
        version_id += 1

        after = event['after_values']
        after['version'] = version_id

        columns = list(after.keys())
        values = [ after[col] for col in columns ]

        ch_connector.insert(
            table=table,
            data=[values],
            column_names=columns
        )

    elif event_type == 'delete':
        statistic.process_event_delete += 1
        logger.debug(f"delete event {schema}.{table} event: {event}")
        deleted_record = event['values']
        version_id += 1
        deleted_record['deleted'] = 1
        deleted_record['version'] = version_id

        columns = list(deleted_record.keys())
        values = [deleted_record[col] for col in columns]
        ch_connector.insert(
            table=table,
            data=[values],
            column_names=columns
        )

    else:
        raise RuntimeError(f"Unknown event type '{event_type}'")
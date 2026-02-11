MYSQL_SETTINGS = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "binlog_reader",
    "password": "strong_pass",
}

MYSQL_SETTINGS_ACTOR = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "binlog_actor",
    "password": "strong_pass",
}

APP_SETTINGS = {
    'db_name': 'mariadb_synch_binlog_tmp_test',
    #init table - this table will scan as initital creating data
    'init_tables': ['items', 'items2'],
    'full_regeneration_threads_count': 20,
    'full_regeneration_batch_len': 10,
    #sync next tables, while parsing binlog
    'scan_tables': ['items','items2'],
    'health_socket': './common/health.sock',
    'binlog_file': './common/binlog.pos',
    'handle_events_plugin': 'plugins_test.plugin_test'
}
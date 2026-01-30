import pytest
from unittest.mock import patch, MagicMock
from src.engine import preflight_check

def test_preflight_ok(fake_cursor):
    fake_cursor.fetchall.side_effect = [
        # SHOW GRANTS
        [("GRANT REPLICATION SLAVE, BINLOG MONITOR ON *.* TO user",)],
        # SHOW VARIABLES
        [
            ("log_bin", "ON"),
            ("binlog_format", "ROW"),
            ("binlog_row_image", "FULL"),
            ("binlog_row_metadata", "FULL"),
            ("server_id", "1"),
            ("binlog_gtid_index", "ON"),
            ("gtid_strict_mode", "ON"),
        ],
    ]

    # cursor.execute вызывается 2 раза
    fake_cursor.execute.side_effect = [
        None,
        None,
    ]

    # не должно падать
    preflight_check(fake_cursor, mysql_settings={}, app_settings={})

def test_missing_replication_slave(fake_cursor):
    fake_cursor.fetchall.side_effect = [
        [("GRANT SELECT ON *.* TO user",)],
    ]

    with pytest.raises(RuntimeError, match="Missing required privileges"):
        preflight_check(fake_cursor, mysql_settings={}, app_settings={})

@patch("src.engine.BinLogStreamReader")
def test_probe_binlog_ok(mock_stream, fake_cursor):
    instance = MagicMock()
    instance.__iter__.return_value = iter([])
    mock_stream.return_value = instance

    fake_cursor.fetchall.side_effect = [
        [
            (
                "GRANT REPLICATION SLAVE, BINLOG MONITOR "
                "ON *.* TO user",
            )
        ],
        [
            ("log_bin", "ON"),
            ("binlog_format", "ROW"),
            ("binlog_row_image", "FULL"),
            ("binlog_row_metadata", "FULL"),
            ("server_id", "1"),
            ("binlog_gtid_index", "ON"),
            ("gtid_strict_mode", "ON"),
        ],
    ]

    preflight_check(fake_cursor, mysql_settings={}, app_settings={})

    instance.close.assert_called_once()
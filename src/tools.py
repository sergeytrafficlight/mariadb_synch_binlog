import os
import re
import importlib

def get_gtid(file_path):
    if not os.path.exists(file_path):
        return None
    gtid_str = open(file_path).read().strip()

    parts = gtid_str.split(",")
    for part in parts:
        if ":" not in part:
            raise RuntimeError(f"⚠ Invalid GTID component (no colon): {part}")
        uuid, txn_id = part.split(":", 1)
        # Проверяем UUID длину и формат (8-4-4-4-12)
        uuid_parts = uuid.split("-")
        if len(uuid_parts) != 5 or \
           [len(p) for p in uuid_parts] != [8,4,4,4,12]:
            raise RuntimeError(f"⚠ Invalid UUID format: {uuid}")
            return None
        # Проверяем, что txn_id — это число
        if not txn_id.isdigit():
            raise RuntimeError(f"⚠ Transaction ID is not a number: {txn_id}")

    return gtid_str


class plugin_wrapper:

    def __init__(self, module_path):
        module = importlib.import_module(module_path)

        self.initiate_full_regeneration = getattr(module, 'initiate_full_regeneration')
        self.finished_full_regeneration = getattr(module, 'finished_full_regeneration')
        self.initiate_synch_mode = getattr(module, 'initiate_synch_mode')
        self.tear_down = getattr(module, 'tear_down')
        self.process_event = getattr(module, 'process_event')


class status_class:

    def __init__(self):
        self.stage
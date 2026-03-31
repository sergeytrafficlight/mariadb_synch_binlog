from threading import Lock, Condition

class version_lock:

    def __init__(self):
        self.version = 1
        self.lock = Lock()

    def get_version(self):
        with self.lock:
            r = self.version
            self.version += 1
            return r

    def set_version(self, value):
        self.version = value

class synch_item:

    def __init__(self, event_type: str, table: str, event):
        self.event_type = event_type
        self.table = table
        self.event = event

class synch_buffer:

    def __init__(self):
        self.insert = {}
        self.update = {}
        self.delete = {}
        self.binlog = None
        self.lock = Lock()

    def len(self):
        total = 0
        with self.lock:
            for k in self.insert:
                total += len(self.insert[k])
            for k in self.update:
                total += len(self.update[k])
            for k in self.delete:
                total += len(self.delete[k])
        return total


    def copy(self):
        new = synch_buffer()
        new.insert = self.insert.copy()
        new.update = self.update.copy()
        new.delete = self.delete.copy()
        if self.binlog:
            new.binlog = self.binlog.copy()
        return new


    def get_event(self):

        def _pop_item(d: dict):
            keys = list(d.keys())
            for k in keys:
                if not len(d[k]):
                    del d[k]
                    continue
                else:
                    key, value = d[k].popitem()
                    return value
            return None


        with self.lock:
            if self.insert:
                value = _pop_item(self.insert)
                if value is not None:
                    return value
            if self.update:
                value = _pop_item(self.update)
                if value is not None:
                    return value
            if self.delete:
                value = _pop_item(self.delete)
                if value is not None:
                    return value
        return None

    def put_binlog(self, binlog):
        if self.binlog is None:
            self.binlog = binlog.copy()
        elif binlog > self.binlog:
            self.binlog = binlog.copy()

    def put_insert(self, table: str, event):
        if table not in self.insert:
            self.insert[table] = {}
        id = event['id']
        #replace if need - it's ok
        self.insert[table][id] = synch_item(event_type='insert', table=table, event=event)

        if table in self.update:
            assert id not in self.update[table]
        if table in self.delete:
            assert id not in self.delete[table]

    def put_update(self, table: str, event):
        if table not in self.update:
            self.update[table] = {}
        id = event['after_values']['id']

        self.update[table][id] = synch_item(event_type='update', table=table, event=event)

        if table in self.insert:
            if id in self.insert[table]:
                del self.insert[table][id]

        if table in self.delete:
            assert id not in self.delete[table]


    def put_delete(self, table: str, event):
        id = event['values']['id']

        if table not in self.delete:
            self.delete[table] = {}
        self.delete[table][id] = synch_item(event_type='delete', table=table, event=event)

        if table in self.insert:
            if id in self.insert[table]:
                del self.insert[table][id]
        if table in self.update:
            if id in self.update[table]:
                del self.update[table][id]



class synch_storage:

    def __init__(self, max_len: int):
        self.lock = Lock()
        self.swap_condition = Condition(self.lock)
        self.buffer = synch_buffer()
        self.max_len = max_len
        self.size = 0

    def put_event(self, event_type, table, event):

        print(f"size: {self.size} max: {self.max_len}")
        with self.lock:
            while self.size >= self.max_len:
                self.swap_condition.wait()

            if event_type == 'insert':
                self.buffer.put_insert(table, event)
            elif event_type == 'update':
                self.buffer.put_update(table, event)
            elif event_type == 'delete':
                self.buffer.put_delete(table, event)
            else:
                raise Exception(f"Unknown event type: '{event_type}'")
            self.size += 1

    def get_buffer(self, expecting_binlog):

        with self.lock:

            self.swap_condition.notify_all()

            if expecting_binlog:
                if self.buffer.binlog is None:
                    return None

            result = self.buffer.copy()
            self.buffer = synch_buffer()
            self.size = 0
            return result


    def put_binlog(self, binlog):
        with self.lock:
            self.buffer.put_binlog(binlog)

    def len(self):
        with self.lock:
            return self.size




class FixedDict(dict):
    """
    Dict with a fixed set of keys and strict key order.
    Prevents adding new keys and ensures all keys were updated.
    """
    def __init__(self, allowed_keys):
        self._allowed_keys = set(allowed_keys)
        self._updated_keys = set()

        super().__init__()

        for key in self._allowed_keys:
            super().__setitem__(key, None)

        self._updated_keys.clear()


    def __setitem__(self, key, value):
        if key not in self._allowed_keys:
            raise KeyError(f"Key '{key}' is not allowed")
        self._updated_keys.add(key)
        super().__setitem__(key, value)


    def update(self, *args, **kwargs):
        for key, value in dict(*args, **kwargs).items():
            self[key] = value

    def check_updated_keys(self):
        if self._allowed_keys != self._updated_keys:
            raise Exception(f"Not all keys has been updated: {self._allowed_keys - self._updated_keys}")



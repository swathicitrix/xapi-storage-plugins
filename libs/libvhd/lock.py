class Lock():
    def __init__(self, opq, name, cb):
        self.opq = opq
        self.name = name
        self.cb = cb

    def __enter__(self):
        self.lock = self.cb.volumeLock(self.opq, self.name)

    def __exit__(self, type, value, traceback):
        return self.cb.volumeUnlock(self.opq, self.lock)

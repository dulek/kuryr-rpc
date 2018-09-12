import Queue
import threading
from locks import RWLock
"""
A set of unit tests for the RPC Protocol
"""

class MockQueue:
    """Crude, but idk, good enough for now
    """


    def __init__(self):
        self._halt_lock = threading.Lock()
        self._is_stopped = False
        self._read_lock = RWLock()
        self._queue = Queue.Queue()

    
    def put(self, value):
        return self._queue.put(value)

    def get(self):
        self._read_lock.reader_acquire()
        val = self._queue.get()
        self._read_lock.reader_release()
        return val
    
    def stop(self):
        self._read_lock.writer_acquire()
        print("Stopping processing")

    def run(self):
        self._read_lock.writer_release()
        print("Resuming Processing")


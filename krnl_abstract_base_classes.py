from krnl_config import os
from abc import ABC

def moduleName():
    return str(os.path.basename(__file__))


class AbstractFactoryBaseClass(ABC):       # Base Animal, Person, Activity Factory, for abstraction.
    def __init__(self):
        super().__init__()


# SQLiteQueueDatabase, AsyncBuffer, and other classes that use the write-cursors from SQLiteQueueDatabase.
class AbstractAsyncBaseClass(ABC):
    # When set, the event signals SQLiteQueueDatabase.stop() that it's ok to stop the writer.
    buffer_writers_stop_events = {}     # {db_name: Event(), }

    def __init__(self):
        super().__init__()

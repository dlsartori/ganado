""" Implements upload to server and sync between devices routines """

from krnl_async_buffer import AsyncBuffer, BufferAsyncCursor


class DataSyncCursor(BufferAsyncCursor):
    """

    """

    def __init__(self, *args, event=None, the_object=None, the_callable=None, **kwargs):
        """
        @param event: Event object that may be passed to signal completion. Not used here.
        @param data: All data to be appended (put) to the queue as a BufferAsyncCursor object.
        @param the_object: the object for which the callable is called. Optional.
        @param the_callable: callable to execute operations on the object. Optional.
        """
        self._args = args  # Data for the object to be operated on (stored, bufferized, etc).
        self._kwargs = kwargs
        super().__init__(event=event, the_object=the_object, the_callable=the_callable)

    def execute(self):
        """ Object-specifc method to process the buffer object (Tx / Rx in this case).
        Called from an independent thread managed by the Writer.
        """
        # TODO: write code here.
        pass

    @classmethod
    def format_item(cls, *args, event=None, the_object=None, the_callable=None, **kwargs):
        """ Item-specific method to put item on the AsyncBuffer queue
        @param event: Event object that may be passed to signal completion. Not used here.
        @param args: All data to be appended (put) to the queue as a BufferAsyncCursor object.
        @param kwargs: Not used here.
        @param the_object: the object for which the callable is called
        @param the_callable: callable to execute operations on the object.
        @return: type(cursor)=cls -> valid cursor. None -> Invalid object: do NOT enqueue().
        """
        # TODO: WRITE formatting code for queue object, including the_object and the_callable if applicable.
        # Creates a cursor with DataSyncCursor.__init__() and sets event and data values.
        return cls(*args, event=event, the_object=the_object, the_callable=the_callable)  # returns cursor.

    def reset(self):
        self._args = []
        self._kwargs = {}

# thread_priority: 0-20. 0 is Highest.
# _upSyncBuffer = AsyncBuffer(DataSyncCursor, autostart=True, precedence=0, thread_priority=12)
# ------------------------------------------------------------------------------------------------------------------ #



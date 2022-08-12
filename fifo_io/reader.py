import os
from time import sleep
from threading import Thread

from ..lock import PLock
from .base import BaseFIFO, Info, get_default_info
from ..exception import EOFError, TimeOutError

TIME_OUT = 60


def read_line(f, container, timeout):
    i = 0
    while i < timeout:
        d = f.readline()
        if d:
            container['data'] = d
            return
        sleep(1)
    raise TimeOutError()


class RDFIFO(BaseFIFO):
    def __init__(self, path, chunksize, async_read=False):
        super().__init__(path, chunksize)
        self.async_read = async_read
        self.r_eof = False
        self.read_lock = PLock(os.path.join(path, 'read_lock'))
        self.info = Info(os.path.join(path, 'read_shared_memory'), chunksize, self.read_lock)
        self.timeout = TIME_OUT
        self.is_closed = False


    def reset(self):
        self.read_lock.acquire()
        self.r_eof = False
        self.info.reset()
        self.read_lock.release()

    def get_f_shared(self):
        d = self.info.get()
        partition, seek = d['data']
        if self._f_partition == partition:
            return self._f, d
        self._f and self._f.close()
        self._f = self._openchunk(partition)
        self._f_partition = partition
        return self._f, d

    def set_block(self, is_sync):
        self.async_read = is_sync

    def set_timeout(self, seconds):
        self.timeout = seconds

    def readline(self):
        if self.r_eof:
            raise EOFError('eof')
        self.read_lock.acquire()
        rf, shared = self.get_f_shared()
        partition, seek = shared['data']
        rf.seek(seek)
        length = rf.readline()
        if not length:
            if self.async_read:
                return None
            container = {'data': None}
            t = Thread(target=read_line, args=(rf, container, self.timeout))
            t.start()
            t.join()
            length = container['data']
        bf_length = int(length.strip())
        d = rf.read(bf_length)
        seek += len(length) + bf_length
        if seek >= shared['chunksize']:
            partition += 1
            seek = 0
            rf.close()
        shared.update({'data': (partition, seek)})
        self.info.update(shared)
        self.read_lock.release()
        if d == self.EOF:
            self.r_eof = True
            raise EOFError('eof')
        return d

    def close(self) -> None:
        if hasattr(self, 'is_closed') and (not self.is_closed):
            rf, shared = self.get_f_shared()
            rf.close()
        self.is_closed = True

    def __del__(self):
        self.close()

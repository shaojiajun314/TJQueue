import os
import mmap
from json import dumps, loads


class BaseFIFO:
    EOF = b'EOF'
    def __init__(self, path, chunksize):
        self.chunksize = chunksize
        self.path = path
        if not os.path.exists(path):
            os.makedirs(path)
        self._f = None
        self._f_partition = None

    def _openchunk(self, number: int, mode: str = 'rb'):
        return open(os.path.join(self.path, 'q%05d' % number), mode)


def get_default_info(chunksize):
    return {
        'chunksize': chunksize,
        'data': [0, 0] # partition, seek
    }


class Info():
    def __init__(self, file_path, chunksize: int, lock):
        wdfd = os.open(file_path, os.O_CREAT | os.O_RDWR)
        self.chunksize = chunksize
        try:
            os.ftruncate(wdfd, 1024)
        except OSError:
            pass
        self._mmap = mmap.mmap(
            wdfd,
            length=1024,
        )
        self.__init_data(file_path, chunksize, lock)

    def __init_data(self, file_path, chunksize, lock):
        lock.acquire()
        d = self._mmap[:].strip(b'\x00')
        if not d:
            self.write_dict(get_default_info(chunksize))
        lock.release()

    def reset(self):
        self._mmap[0:1024] = b'\x00'*1024
        self.write_dict(get_default_info(self.chunksize))

    def get(self):
        raw = self._mmap[:]
        return loads(raw.strip(b'\x00').decode())

    def update(self, data):
        self.write_dict(data)

    def write_dict(self, d):
        d = dumps(d).encode()
        self._mmap.seek(0)
        assert len(d) == self._mmap.write(d)

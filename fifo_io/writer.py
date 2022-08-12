from .base import get_default_info, BaseFIFO


class WTFIFO(BaseFIFO):
    def __init__(self, path, chunksize):
        self.is_closed = False
        super().__init__(path, chunksize)
        self.info = get_default_info(chunksize)
        self.chunksize = chunksize
        self.wf = self._openchunk(self.info['data'][0], 'ab+')
        self.w_eof = False

    def writeline(self, string) -> None:
        assert isinstance(string, bytes), 'error type'
        partition, seek = self.info['data']
        bf = str(len(string)).encode() + b'\n' + string
        self.wf.write(bf)
        seek += len(bf)
        if seek >= self.chunksize:
            partition += 1
            seek = 0
            self.wf.close()
            self.wf = self._openchunk(partition, 'a+')
        self.info['data'] = [partition, seek]

    def flush(self):
        self.wf.flush()

    def close(self) -> None:
        if not self.is_closed:
            # self.writeline(self.EOF)
            self.wf.close()
        self.is_closed = True

    def end(self):
        self.writeline(self.EOF)
        self.flush()

    def __del__(self):
        self.close()

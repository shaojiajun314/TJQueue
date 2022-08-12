from time import sleep, time
from threading import Thread, Lock
from .fifo_io import WTFIFO, RDFIFO
from .config import fifo_disk_queue_dir

L = Lock()


QUEUE_DEL_DUR = 60
FLUSH_DUR = 5


def FifoDiskQueue(name, chunksize=2000000):
    return WTFIFO(name, chunksize), RDFIFO(name, chunksize)


class Queue():
    def __init__(self, name):
        if isinstance(name, bytes):
            # TODO: error
            name = name.decode()
        self.w, self.r = FifoDiskQueue(fifo_disk_queue_dir / name)
        self.is_closed = False
        self.destroy_time = time() + QUEUE_DEL_DUR
        self.flush_time = None

    def push(self, msg):
        self.destroy_time = time() + QUEUE_DEL_DUR
        self.w.writeline(msg)
        if not FLUSH_DUR:
            self.w.flush()
            return
        self.flush_time = time() + FLUSH_DUR

    def flush(self):
        self.w.flush()

    def end(self):
        self.w.end()

    def reset_reader(self):
        self.r.reset()

    def pop(self):
        self.destroy_time = time() + QUEUE_DEL_DUR
        return self.r.readline()

    def close(self):
        if (hasattr(self, 'is_closed')) and (not self.is_closed):
            self.w.close()
            self.r.close()
        self.is_closed = True

    def __del__(self):
        self.close()


QUEUE_MAP = {}


def get_queue(qname):
    with L:
        queue = QUEUE_MAP.get(qname)
        if queue:
            queue.destroy_time = time() + QUEUE_DEL_DUR
            return queue
        QUEUE_MAP[qname] = Queue(qname)
    return QUEUE_MAP[qname]


class QueueGC(Thread):
    instance  = None
    def __new__(cls, *args, **kw):
        with L:
            if cls.instance:
                return cls.instance
            cls.instance = super().__new__(cls)
            return cls.instance

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.started = False

    def run(self):
        with L:
            if self.started:
                return
            self.started = True
        while 1:
            if not QUEUE_MAP:
                sleep(1)
                continue
            t, q_name = min([
                (q.destroy_time, qname)
                for qname, q in QUEUE_MAP.items()
            ])
            sleep_time = t - time()
            if sleep_time <= 0:
                with L:
                    del QUEUE_MAP[q_name]
                continue
            sleep(sleep_time)


class QueueFlusher(Thread):
    instance  = None
    def __new__(cls, *args, **kw):
        if cls.instance:
            return cls.instance
        cls.instance = super().__new__(cls)
        return cls.instance

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.started = False

    def run(self):
        if self.started:
            return
        self.started = True
        while 1:
            tmp = [
                (q.flush_time, qname)
                for qname, q in QUEUE_MAP.items()
                if q.flush_time != None
            ]
            if not tmp:
                sleep(1)
                continue
            t, q_name = min(tmp)
            sleep_time = t - time()
            if sleep_time <= 0:
                QUEUE_MAP[q_name].flush()
                QUEUE_MAP[q_name].flush_time = time() + FLUSH_DUR
                continue
            sleep(sleep_time)


QueueGC(daemon=True).start()
QueueFlusher(daemon=True).start()

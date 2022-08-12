import os
from pathlib import Path

fifo_disk_queue_dir = Path(os.getenv('FifoDiskQueueDir', 'var'))
if not os.path.exists(fifo_disk_queue_dir):
    os.makedirs(fifo_disk_queue_dir)

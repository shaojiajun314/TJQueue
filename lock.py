# import os
# import posix_ipc
# from hashlib import md5
#
#
# class PLock():
#     lockprefix = '/tjj_p_lock'
#     def __init__(self, related_file_path):
#         self.s =  posix_ipc.Semaphore(
#             f'{self.lockprefix}-{md5(related_file_path.encode()).hexdigest()[:-15]}',
#             flags=os.O_CREAT,
#             initial_value=1
#         )
#
#     def acquire(self):
#         self.s.acquire()
#
#     def release(self):
#         self.s.release()
#
#     def __del__(self):
#         if hasattr(self, 's'):
#             self.s.close()


import fcntl


class PLock():
    def __init__(self, file_path):
        self.lock_fd =  open(file_path, 'w').fileno()

    def acquire(self):
        fcntl.flock(self.lock_fd, fcntl.LOCK_EX)

    def release(self):
        fcntl.flock(self.lock_fd, fcntl.LOCK_UN)
